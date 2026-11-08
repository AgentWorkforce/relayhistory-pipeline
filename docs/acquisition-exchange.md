# Acquisition exchange (`SERVICE=acquisition`)

Status: **dormant**. Default off, no deployment, no migration applied. **Verify-only**: this
service never mints, signs or stores acquisition credentials (see "Issuer boundary"). It
implements the History side of the acquisition exchange contract v1 operations port on top of
the design, PR #314 `docs/factory-acquisition-owner-design.md` (§3, §3.1, §4.1, §4.2, §6, §8).
The A/S codec and verifier are the peer module `src/session-ingest/acquisition-assertion.ts`
(#322, `docs/acquisition-assertion.md`); the four SQL functions come from the SQL lane
(#332, migration `20261105043850`).

Code: `src/acquisition/` (`app.ts` routes and auth, `config.ts` env, `installer.ts` SQL
adapter, error table and ops handler, `key-registry.ts` pinned public keys, `verifier.ts`
acquisition token verifier, `main.ts` listeners).

## Issuer boundary

Decision of the repo owner (Khaliq, 2026-11-08), superseding this PR's original native issuer
design:

- relaycontrol is the sole issuer of acquisition assertions and credentials: install, revoke,
  rebind, and any pre-signed or reserved revoke.
- The pipeline only verifies (relaycontrol introspection, or relaycontrol's published keys
  pinned in `ACQUISITION_PINNED_KEYS`) and forwards to the privileged installer.
- `ACQUISITION_PINNED_KEYS` accepts only `cloud`-class keys. A `native`-class entry is refused
  at startup (alone or mixed with valid cloud entries): there is no native issuer class, and
  relaycontrol is the sole issuer.
- The SQL installer boundary (#332, migration `20261105043850`) does not itself authenticate
  signatures; this service's verification is what stands in front of it.

There is no issuer key, intent store, native exchange or reconciler in this service. The
issuer-era variables `ACQUISITION_ISSUER_PRIVATE_KEY`, `ACQUISITION_ISSUER_KEY_ID` and
`ACQUISITION_INTENT_KEY` refuse startup if set (even empty, even with `ACQUISITION_ENABLED`
off), and so does `ACQUISITION_NATIVE_ENABLED` set to anything but empty or `false`. The former
native URLs (`/v1/acquisition/native/*`) are not served: `404 not_found` on the ops listener,
`503 acquisition_disabled` on the dormant listener. Native Desktop enrollment moves to
relaycontrol; its issuing endpoint, scope and audience contract is owned by layered-control.

### Tracked gap: native receiver contract

relaycontrol emitting native (Desktop) install, rebind or revoke assertions cannot use the ops
route unchanged: `main.ts` constructs the `Installer` with the cloud class only, and
`ops/rebind` is documented as always 403 because rebind was native-only. Native Desktop
enrollment remains unimplemented until a concrete issuer and receiver path exists.

- Issuing side owner: relay-control (layered-control).
- Receiver path (a dedicated verification class and scope) is to be designed in a follow-up PR
  in this repo.
- Do not restore pipeline signing, and do not broadly trust cloud-class issuance for native.

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

The ops routes authenticate through the purpose-specific acquisition verifier
`src/acquisition/verifier.ts` (`createAcquisitionVerifier`). It keeps the #313 wire contract
(endpoint, request body, service secret, response shape, size cap, outage mapping) by reusing
the unchanged exports of `packages/pipeline/src/auth/relaycontrol.ts`, and differs only in
policy:

| Route group | Token prefix | principal_type | Scope |
|---|---|---|---|
| ops (and the future Q3 route) | `sh_wk_` (§9.1 per-workspace service token) | `service` | `acquisition:operate` |

`verifier.ts` is unchanged from the original design and still carries a `native` policy row
(`sh_at_`, `desktop-user`, `rth:acquire`); no route uses it.

- A token with another prefix is refused before introspection.
- The real `principal_type` is returned; an unknown or absent one is refused (fail closed).
- Scopes are returned raw (no allowlist); the route check asks for exactly its one scope.
- `aud` = `ACQUISITION_ORIGIN`, compared by normalized origin. No cache: every request
  introspects, bounded by the timeout.

The shared #313 client is unchanged and is no longer used by this service.

## Process model

One container, two listeners, so the private operations port is never reachable through
a public ingress:

| Listener | Env | Routes | Exposure |
|---|---|---|---|
| dormant | `ACQUISITION_PORT` (0.0.0.0:8080) | `/healthz` only; everything else `503 acquisition_disabled` | none required |
| ops | `ACQUISITION_OPS_HOST`:`ACQUISITION_OPS_PORT` (127.0.0.1:8082) | `/v1/acquisition/ops/*`, `/healthz` | loopback only by default |

The dormant listener binds `0.0.0.0` and serves no acquisition route in any configuration.
The ops listener binds `ACQUISITION_OPS_HOST`, default
`127.0.0.1`, and its port defaults to **8082** because 8081 is the VPC worker health port
(`stacks/aws/proof-addons.ts:229-230`). **Exposing the ops listener beyond loopback (setting
`ACQUISITION_OPS_HOST` to a pod or VPC address) needs a NetworkPolicy change** that admits
only the relaycontrol/Cloud caller; it is never routed through the Desktop ingress.

Every ops route answers `503 {"error":{"code":"acquisition_disabled"}}` (with
`Retry-After: 60`) unless `ACQUISITION_ENABLED=true` and `ACQUISITION_OPS_ENABLED=true`.
`/healthz` always answers `{ok, enabled, group}`.

## Environment

All configuration is environment-only; nothing is read from disk. With
`ACQUISITION_ENABLED` unset or `false`, only the listener variables are read
(`ACQUISITION_PORT`, `ACQUISITION_OPS_PORT`, `ACQUISITION_OPS_HOST`), because the listeners
still bind to answer `503 acquisition_disabled` and `/healthz`; an invalid value fails startup
even when disabled. The issuer-era variables are checked for presence only and refuse startup
(see "Issuer boundary"). No other variable is read. Booleans are strict
(`true` / `false` / unset). Secrets are read once at startup and never logged.

| Variable | Required when | Meaning |
|---|---|---|
| `ACQUISITION_ENABLED` | - | Master flag, default `false`. |
| `ACQUISITION_OPS_ENABLED` | - | Operations port group, default `false`. |
| `ACQUISITION_PORT` / `ACQUISITION_OPS_PORT` | always | Listener ports, default 8080 / 8082 (8081 is the worker health port), must differ. |
| `ACQUISITION_OPS_HOST` | always | Ops bind address (IP literal), default `127.0.0.1`. Anything else needs a NetworkPolicy change. |
| `ACQUISITION_ORIGIN` | enabled | This service's public origin (URL, origin only). Sent as `aud` to relaycontrol and compared by normalized origin. |
| `RELAYCONTROL_INTROSPECT_URL` | enabled | #313 introspection endpoint; checked against the destination policy. |
| `RELAYCONTROL_INTROSPECT_SECRET` | enabled | #313 service secret (secret; 1Password). |
| `ACQUISITION_PINNED_KEYS` | enabled | JSON array of `{issuerKeyId, issuerClass:"cloud", publicKeyBase64}` (raw 32-byte Ed25519). Only `cloud`-class keys are accepted; a `native`-class entry (or any other class) is refused at startup (see "Tracked gap: native receiver contract"). Ids are unique and one public key may not be pinned under two ids; startup fails otherwise. |
| `ACQUISITION_DATABASE_URL` | enabled | Installer login, exactly `postgresql://user:password@host[:port]/database` (secret; 1Password). The user must be `bulls96_acquisition_installer` (or the pooler form `bulls96_acquisition_installer.<tenant>`), the password non-empty, and the only allowed parameter is `sslmode=verify-full`; `sslmode=disable`/`no-verify`/anything else, `options`, `application_name` or any other parameter refuses startup. Parsed into discrete pg fields (`src/acquisition/db-login.ts`, reusing `src/shared/owner-pg-config.ts`), never passed as a `connectionString`, so no `PG*` variable or `.pgpass` is consulted. `application_name` is fixed (`96bulls-acquisition-installer`) and `options` is fixed to `-c statement_timeout=5s -c lock_timeout=2s -c idle_in_transaction_session_timeout=10s`. |
| `ACQUISITION_DATABASE_CA_PEM` | enabled | PEM CA certificate for the database; TLS is always verify-full against it (server name = URL host). A private key or unparseable PEM refuses startup. |
| `DESTINATION_*` | as the shared policy | `src/shared/destination-url.ts` policy for the introspection URL. |

**Installer login check.** With `ACQUISITION_OPS_ENABLED=true`, before the ops listener starts
(and so before `/healthz` answers), the service reads the live session and refuses to start
unless: `session_user` and `current_user` are exactly `bulls96_acquisition_installer`, the
`role` setting is `none` (no `SET ROLE`), this backend's `pg_stat_activity.usename` is
`bulls96_acquisition_installer` on every version (`backend_user`: a superuser's
`SET SESSION AUTHORIZATION` leaves the three previous signals looking genuine, and before PG16
usename is the only one that still names the authenticated login), on PG16+ `system_user` is `<method>:bulls96_acquisition_installer`
(NULL, i.e. trust auth, is refused), the role is not superuser, `BYPASSRLS`, `CREATEROLE`,
`CREATEDB`, `REPLICATION` or `INHERIT`, it is a member of no role (`role_membership`), the role
itself has no member (`role_members`, see below), and the effective
`statement_timeout` / `lock_timeout` / `idle_in_transaction_session_timeout` are `5s` / `2s` /
`10s`. Refusals name only the failed property (`96bulls: acquisition installer login refused: <code>`).

Inbound edges are checked as well as outgoing ones because any LOGIN granted `bulls96_acquisition_installer`
could `SET ROLE` to it and call the SECURITY DEFINER installer RPCs. On PG16+ the only tolerated
member row is the Supabase creator edge that PostgreSQL adds itself when Supabase's
non-superuser `postgres` (CREATEROLE) creates the role: member `postgres`, grantor the superuser
`supabase_admin`, `admin_option` true, `inherit_option` false, `set_option` false. That edge
can neither `SET ROLE` to the installer nor inherit its privileges. It is the same predicate as the
installer role verifier and bootstrap (`supabase/bootstrap/acquisition_installer_verify.sql`, #332). Before PG16 `pg_auth_members` has no
`inherit_option` / `set_option` columns, so any member refuses startup.

## Authentication

Bearer only, introspected through `verifier.ts` (see "Verifier in use") with
`aud = ACQUISITION_ORIGIN`. Identity (org, workspace, user) comes **only** from the
introspection answer; no client header is forwarded. Introspection unavailable or timed out:
`503 introspection_unavailable` (fail closed).

| Group | principal_type | scope | Refusals |
|---|---|---|---|
| ops | `service` (per-workspace service token) | `acquisition:operate` | **every** authentication refusal is 403 `not_authorized`, a missing bearer included (contract P2) |

Ops tokens are per-workspace: an envelope or status body whose `orgId`/`workspaceId` differ
from the introspected ones is `403 {"error":{"code":"target_mismatch"}}` before any
verification or SQL.

### Pending registration (layered-control) and relaycontrol prerequisites

The scope `acquisition:operate` (service principal) is new and **pending registration by
layered-control**. Until it is registered, and until the acquisition origin
(`ACQUISITION_ORIGIN`) is listed in `SELFHOST_INTROSPECT_AUDIENCES` on each deployment, every
acquisition request fails closed.

This service no longer depends on the shared #313 client's scope allowlist or principal
filter (see "Verifier in use").

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

The envelope is verified by the peer verifier against the cloud-class pinned keys (relaycontrol's
published keys) and only then forwarded to the SQL installer. Note: the peer codec requires
rebinds to be native-class, so `ops/rebind` cannot succeed with a cloud key (403).

### Error table

| Cause | HTTP | Body |
|---|---|---|
| malformed body, 22023 | 400 | `{error:{code}}` (`invalid_request`, `request_digest_mismatch`, ...) |
| unknown key, bad signature, wrong issuer class, 42501 | 403 | `{error:{code}}` (`not_authorized` = bearer/scope/signature check; `target_mismatch`, ...) |
| 55000, 40001 | 409 | `{error:{code, retryable}}` |
| unknown route, including the former `/v1/acquisition/native/*` | 404 | `{error:{code:"not_found"}}` |
| request target over 8 KiB | 414 | `{error:{code:"request_uri_too_long"}}` |
| database / transport failure | 503 | `{error:{code:"acquisition_unavailable", retryable:true}}` |
| introspection unavailable | 503 | `{error:{code:"introspection_unavailable"}}` |
| `ACQUISITION_ENABLED` or `ACQUISITION_OPS_ENABLED` off, or the dormant listener | 503 | `{error:{code:"acquisition_disabled"}}`, `Retry-After: 60` (other 503s: `Retry-After: 5`) |

Only `session_busy` and transport-class failures are `retryable:true`; `revision_cas_lost`,
`stale_revision`, `predecessor_mismatch`, `operation_conflict`, `origin_cas_lost` and every
55000 code are `retryable:false` (re-read status, build a new operation). The stable code is
the message after `acquisition: `; a message outside the contract, or a well-formed code that is
not in its SQLSTATE's list above (an unknown future code, or a known code under another
SQLSTATE), is a defect (500, not retryable), never a 4xx/409 refusal.

## Logging

Only allowlisted fields go through `src/shared/log.ts`: route, correlation id, status,
outcome, error code, retryable, duration, org/workspace ids. Never the bearer, the
introspection secret, the envelope signature or any key material.
