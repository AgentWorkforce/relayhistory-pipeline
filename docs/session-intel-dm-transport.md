# Session intelligence: the DM transport contract (M3-8)

Contract notes, 2026-11-19. Owner: sessions-intel-lead. Sources:

- layered-relaycast, which read the relaycast engine (origin/main 99a81413), the hosted gateway and a
  Cloud snapshot of 2026-11-17;
- layered-relay-app, for the Desktop side.

Nothing has been registered, issued or sent. This is the contract the advisor's sender and its test fake
build against. It replaces the unverified payload in `src/coordination-advisor/relay-sender.ts`.

## Who does what

| Step | Owner | Mechanism |
|---|---|---|
| Resolve a (harness kind, session id) to an agent | Cloud | `desktop_relay_identities` holds the native session kind and id next to the agent name and address, written by Desktop's identity sync. Relaycast cannot resolve a session. The read path for a service is **not confirmed**; its owner is Khaliq's call |
| Register the sender | Relaycast | `POST /v1/agents` with the customer workspace's key. There is no separate service-identity path, so whoever holds the workspace key registers it. That needs Khaliq's OK, or the customer's |
| Send | Relaycast | `POST /v1/dm` with the sender's own agent token |
| Check delivery | Relaycast | `GET /v1/messages/:id/deliveries` |

## Registering the sender

- **Request:** `POST /v1/agents`, body `{name, type, persona, metadata, ...}`. `type` is exactly
  `agent`, `human` or `system`; use `system`. Names are not pattern-checked. Reserved names give
  `400 invalid_agent_name`.
- **Response:** `201` with `id`, `workspace_id`, `name`, `address`, `handle`, `token` and `status`. The
  agent token is returned only here, and the server stores a hash. It goes to 1Password and never to
  disk.
- **Name already taken:** `409 agent_already_exists`, a hard conflict. The server never suffixes or
  reuses the name.
- **Token rotation:** `POST /v1/agents/:name/rotate-token` with the agent's own token gives
  `{name, token}`. The old token works for 60 s more. The workspace owner can take over, revoke or
  delete the agent with the workspace key.
- **Expiry:** tokens have no expiry today, but a draft change would add one, so do not rely on that.
- **Agent caps:** hosted plans cap registered agents. Over the cap, registration returns
  `429 plan_limit_exceeded`.

## Sending

- **Request:** `POST /v1/dm`, body with exactly one of `to` (an agent name) or `address`
  (`agent@machine`), plus:
  - `text` (required);
  - `data` (an object; keys starting with `__relaycast_` are stripped);
  - `mode` (`wait`, the default, or `steer`);
  - `reply_to_message_id`;
  - `wait_ms` (0 to 2000).
- **Idempotency:** send an `Idempotency-Key` header of at most 255 characters, remembered for 24 h, and
  set it to the advisory id plus the recipient. A retry with the same key and a different body gets
  `409 idempotency_key_reused`.
- **Response:** `201` with the message and a `delivery` object. `delivery.state` is `queued`,
  `delivered`, `consumed`, `replied` or `failed`.
- **Size:** no limit was found, so keep advisories within the existing 600-character template.

### Rate limits

- The limit is per workspace per minute, shared by every sender in the workspace. `POST /v1/dm` gets
  half of the plan's ceiling.
- `429 rate_limit_exceeded` comes with `Retry-After` and the `X-RateLimit-*` headers.
- `429 workspace_delivery_depth_exceeded` (`Retry-After: 30`) means the workspace's undelivered
  backlog is full.

### Errors

Errors come as `{ok:false, error:{code, message}}`:

| Status | Codes |
|---|---|
| `400` | `invalid_request`, `invalid_json`, `invalid_idempotency_key`, `invalid_address`, `ambiguous_address`, `invalid_attachments` |
| `401` | `unauthorized`, `agent_token_invalid`, `workspace_expired` |
| `404` | `agent_not_found`, `address_not_found` |
| `409` | `idempotency_key_reused`, `idempotency_in_progress`, `dm_conversation_id_collision` |
| `429` | the two codes above |
| `503` | `idempotency_unavailable`, `database_overloaded` |
| `500` | `internal_error` |

## Offline and not attached

- **No agent for the session:** Cloud has no agent for the (kind, session id). This is the "not
  attached" outcome. Nothing is sent, and the advisory stays on the pull path.
- **Unknown name:** `404 agent_not_found`. Treat it as not attached.
- **Recipient offline:** the DM is accepted and stays `queued`. A queued delivery expires after 1 hour
  and then reads `failed`.
- **Recipient mailbox full:** `delivery` is `null`, though the message is stored.
- **Advisor rule:** an advisory is *delivered* only when `delivery.state` reaches `delivered` or later.
  `queued` after 10 minutes counts as not delivered, and the row stays on the pull path.

## What the advisor does with this

1. Resolve each recipient through the Cloud lookup. No agent means not attached: no DM.
2. Send with the advisory-scoped idempotency key, within the workspace rate limit. Back off on either
   429 using `Retry-After`. The existing per-recipient cap and cooldown still apply.
3. Mark `delivered_at` only once the delivery state is `delivered` or later. A `queued` row stays
   pending on the pull path.

The test fake models:

- `409` on a duplicate registration;
- both 429s with `Retry-After`;
- `404 agent_not_found`;
- the 60 s rotation grace;
- the delivery state moving from `queued` to `delivered` to `failed`.

## Still open

- **The Cloud read path** from (kind, session id) to an agent, for a service caller: who owns it, and
  what endpoint and auth it uses.
- **Registration and the agent token** in our workspace. This is Khaliq's OK, and for a customer, the
  customer's.
- **Plan caps:** whether a `system` agent counts against the registered-agent cap.
