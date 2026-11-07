# Session-ingest edge (self-host Desktop uploads, relaycontrol tokens)

`SERVICE=session-ingest-edge` (`src/session-ingest-edge/`) is the self-host
replacement for relayhistory-cloud's signing edge (`lib/delivery-forward.ts`
there, `docs/session-ingest-design.md` section 2). It sits in front of the
session-ingest Worker (`src/session-ingest/app.ts`), takes Agent Relay
Desktop's `Authorization: Bearer sh_at_...`, has relaycontrol introspect it
(no RelayAuth), and forwards the request with the signed
`x-relay-ingest-identity` the Worker already verifies. The Worker is unchanged.

```
Desktop --Bearer sh_at_--> edge --{token, aud}--> relaycontrol /internal/v1/auth/introspect
                             \--x-relay-ingest-identity + x-relay-ingest-signature--> Worker
```

## Default off

`SESSION_INGEST_EDGE_ENABLED` follows the repo's strict-flag, dormant-service
pattern (the Worker's own "every request 503 `not_configured`" when unconfigured,
with the strict `true`/`false` parsing of `ALLOW_IN_CLUSTER_HTTP`):

- unset, empty or `false`: dormant. The process starts and reads no other
  variable except `SESSION_INGEST_EDGE_PORT` (to listen; a malformed port still
  fails startup). `GET /healthz` answers `200 {"ok":true,"enabled":false}` and every
  other route answers `503 not_configured`. Nothing is introspected or forwarded.
- `true`: enabled. Every variable below is required and validated at startup.
- anything else: the process refuses to start (a typo never silently means either).

## Environment

| Variable | Meaning |
| --- | --- |
| `SESSION_INGEST_EDGE_ENABLED` | `true` to enable; default off (above). |
| `SESSION_INGEST_EDGE_ORIGIN` | This edge's public origin, exactly `scheme://host[:port]` (lowercase, no path, no trailing slash, no default port). Sent as the introspection `aud`; relaycontrol's echo must normalize to the same origin (the #313 client's `normalizedAudience`, also used to validate this value). Must be listed in relaycontrol's `SELFHOST_INTROSPECT_AUDIENCES`; see [Relaycontrol audiences](#relaycontrol-audiences-operator). |
| `SESSION_INGEST_UPSTREAM_URL` | The session-ingest Worker, a bare origin (no path). https, loopback, or an in-cluster Service name (`<svc>.<ns>.svc[.cluster.local]`) with `ALLOW_IN_CLUSTER_HTTP=true` (`src/shared/destination-url.ts`). |
| `RELAYCONTROL_INTROSPECT_URL` | relaycontrol's base URL (same transport rule); the client appends `/internal/v1/auth/introspect`. |
| `RELAYCONTROL_INTROSPECT_SECRET` | The dedicated introspection service secret, from the secret store (1Password); never on disk. |
| `INGEST_SIGNING_KEY` | The HMAC key the Worker verifies (`signRequest`), from the secret store. **This deployment's own key, never the cloud ingest signing key** (see below). |
| `SESSION_INGEST_EDGE_PORT` | Listen port, default 8080. |
| `ALLOW_IN_CLUSTER_HTTP` | Existing switch: allows plain http to in-cluster Service names. |

**Signing key isolation.** The edge signs identities it takes from
relaycontrol's org and workspace namespace with the same HMAC scheme the cloud
signing edge uses. Give each self-host deployment its own `INGEST_SIGNING_KEY`,
shared only between this edge and its own Worker, and never reuse the cloud
ingest signing key: a shared key would let a self-host edge (or anyone who
reads its secret) mint identities that a cloud Worker accepts.

**Probes.** Point liveness and readiness probes at `GET /healthz` (200 when
dormant and when enabled). The edge has no `/readyz`, like the Worker; a probe
there would get 404 when enabled and 503 when dormant.

Startup errors name the variable, never its value. The edge makes no outbound
call except to these two configured URLs.

## Relaycontrol audiences (operator)

relaycontrol only marks a token active for an `aud` in its
`SELFHOST_INTROSPECT_AUDIENCES`: a comma-separated list, each entry normalized
to `scheme://host[:port]` (lowercased, path and trailing slash dropped, default
port dropped). When it is **unset**, the list is `[RELAYHISTORY_PUBLIC_URL]`
only, so the edge's audience is not on it and every Desktop upload is refused
as `audience` (401 `invalid_token`).

With the edge enabled, list **both** origins:

- the History public URL (`RELAYHISTORY_PUBLIC_URL`; what the read API and
  pipeline send as `aud`), and
- `SESSION_INGEST_EDGE_ORIGIN` (what this edge sends).

Example (placeholders):
`SELFHOST_INTROSPECT_AUDIENCES=https://history.example.test,https://ingest.example.test`.

Optional preflight before setting `SESSION_INGEST_EDGE_ENABLED=true`: from
inside the cluster, `GET` relaycontrol's `/readyz` and check that its
`introspection.audiences` contains `SESSION_INGEST_EDGE_ORIGIN` (normalized).
The edge does not run this check itself; it is an operator step.

## Routes (Amendment 4)

`EDGE_ROUTES` in `src/session-ingest-edge/app.ts` is the single source of truth.
Exact method and path; everything else is `404 not_found` before anything else
is looked at, and is never introspected or forwarded.

| Method | Path | Edge scope |
| --- | --- | --- |
| POST | `/v1/delivery/batches` | `rth:sync` |
| POST | `/v1/turns/batches` | `rth:sync` |
| POST | `/v1/delivery/sessions/selection` | `rth:sync` |
| POST | `/v1/delivery/sessions/reset` | `rth:sync` |
| POST | `/v1/onboarding/heartbeat` | `rth:sync` |
| GET | `/v1/onboarding/connection` | `rth:read` |
| GET | `/v1/delivery/limits` | `rth:read` |

Explicitly **not** forwarded: `/v1/delivery/recovery` and every `/v1/custody/*`
route. They carry cloud-issuer custody witnesses that the Worker authenticates
only by the outer HMAC, which a user token must never be able to obtain. The
Worker applies its own scope rule after the edge: it still requires `rth:sync`
for `GET /v1/delivery/limits`, so Desktop tokens need both scopes there.

## What is forwarded

- **Body:** a POST must be `Content-Type: application/json` (media type compared
  case-insensitively after parameters are stripped); anything else, including
  the custody and recovery content types, is `415` before introspection.
  `Content-Encoding` may be identity, gzip or zstd (else `415`) and is passed
  through with the exact wire bytes; the Worker decodes and bounds it. The wire
  body is capped at `DELIVERY_LIMITS.maxRequestBytes` (2 MiB): a larger declared
  `Content-Length` is `413` before introspection, and the streamed read stops
  at the cap (`413`). `origin_id` and everything else in the body are untouched.
- **Identity:** only from relaycontrol's answer: `sub` -> `userId`, `org` ->
  `orgId`, `workspace` -> `workspaceId`, `scopes` mapped through the shared
  client's allowlist (`rth:read`, `rth:sync` only). Never `control`, never from
  a request header, query or body.
- **Headers:** built from scratch: the identity, its signature, `Content-Type`
  and `Content-Encoding` (POST), `X-Correlation-Id`, `Accept`. A client's
  `x-relay-ingest-identity`, `x-relay-ingest-signature`, `Authorization`,
  cookies, `X-Machine-Id` or workspace header never reach the Worker. The query
  string is dropped (the signature covers the path only).
- **Response:** the Worker's status, body and headers stream back, minus
  hop-by-hop headers, `Content-Length`, `Content-Encoding` and `Set-Cookie`. `X-Request-Id` is the edge's id;
  `X-Correlation-Id` is the client's (if it matches `[A-Za-z0-9._:-]{1,128}`)
  or the request id.

## Introspection and status mapping

The edge reuses the shared client `packages/pipeline/src/auth/relaycontrol.ts`
(one cache per process): request `{token, aud}` only (no `workspace`, no
`machine`; contract pin relay-control #20 `28e9d95`), 5 s timeout, positive
cache at most 60 s and never past `exp`, negative cache at most 5 s, keyed by
sha256(token) + aud, single-flight.

| Condition | Edge answer |
| --- | --- |
| route not in `EDGE_ROUTES` | 404 `not_found` |
| POST not `application/json`, custody/recovery type, unsupported encoding | 415 |
| declared or streamed body over 2 MiB | 413 `delivery_too_large` |
| no bearer | 401 `missing_authorization` |
| bearer not `sh_at_` | 401 `invalid_token` (relaycontrol not called) |
| `active:false` (any reason), principal not `desktop-user`, audience mismatch (normalized) | 401 `invalid_token` |
| relaycontrol 400 (our request malformed), 401 (our secret refused; `introspect_unauthorized`), 404 (introspection disabled), 16 KiB answer cap, 5xx, network, timeout, malformed answer | 503 `relaycontrol_unavailable`, `Retry-After: 1` (as the #313 paths) |
| token lacks the route's scope | 403 `forbidden` |
| Worker unreachable | 503 `delivery_unavailable` |
| dormant | 503 `not_configured` |

Every refusal forwards nothing. Logs carry route, outcome, status, org and
workspace ids and the correlation id; never the bearer, the introspection
secret, the signing key, the identity header or the body.

Whenever relaycontrol answers 400, 401 or 404 the edge surfaces **503**
`relaycontrol_unavailable`, never 401: each is a deployment fault (our request,
our secret, or introspection switched off), not a verdict on the caller's
token, so Desktop must retry rather than discard a valid token. Only an
`active:false` answer, a non-`desktop-user` principal or an audience mismatch
is a 401.

## Extension point: origin_id

`originIdGate` in `app.ts` is the one place a future verification of the
body's `origin_id` (the Desktop install) would plug in. Today it does nothing:
the edge adds no machine gate and assumes none is absent. It runs after
authentication and the scope check, before signing.
