# R2 control-head contract v1 (Option C privacy gate) — 2026-11-08

Pinned by pipeline-lead. Producer: relayhistory-cloud (relayhistory-cloud-owner). Consumer: sessions-pipeline session read Worker (layered-sessions-pipeline). Adversarial: neonexit-review-dogpatch, codex-relay-desktop-find-prior (fixtures v2 `b5846258…`).

## Transport
- Cloudflare **service binding only** (`HISTORY_CONTROL_HEAD` on the read Worker → relayhistory-api). No public route; the handler refuses any request not arriving via the binding (no `cf-connecting-ip` / public hostname path registered).
- `POST /internal/v1/privacy/control-head`, `content-type: application/json`, body ≤ 4 KiB.
- Caller identity: the binding itself (service-to-service). The **tenant is not trusted from any end-user input**: the read Worker sends the identity it derived from the verified RelayAuth token; relayhistory treats the binding as the trust boundary.

## Request
```json
{ "contract": "r2-control-head/v1", "org": "<orgId>", "workspace": "<workspaceId>", "user": "<userId>" }
```
- Exactly these keys; any other key, empty/over-128-char value → `400 invalid_request`.
- One user per request (no batch). Session-level revocation history does not exist in Neon, so per-session queries add nothing.

## Response 200
```json
{
  "contract": "r2-control-head/v1",
  "org": "<echo>", "workspace": "<echo>", "user": "<echo>",
  "cutover": { "epoch": "<decimal string>", "mode": "ingest" },
  "head": null | { "epoch": "<decimal string>", "version": "<decimal string>", "digest": "<hex64>", "kind": "selection|reset|...", "pending": true|false },
  "observedAt": "<RFC3339 UTC>"
}
```
- Source: `cutover` from the live `DELIVERY_CUTOVER` state relayhistory already reads; `head` from `sessions.delivery_cutover_control_heads` for exactly (org, workspace, user) — read-only, one statement, `statement_timeout` ≤ 2 s.
- `head = null` means **no control row exists for this user in this cutover epoch** (selection, reset, and any other control kind all write this head per 0078). Missing user/workspace is not an error: it is `head: null`.
- All epochs/versions are **decimal strings** (consumer parses as BigInt, never JS Number).
- No caching (`cache-control: no-store`); no cross-request memoization.

## Errors
- `400 invalid_request`, `403 not_binding`, `503 neon_unavailable` (timeout/connection), `500 internal`. Body `{ "error": { "code": "<code>", "reason": "<code>" } }`. Any non-200, timeout (> 2.5 s at the caller), shape mismatch, or echo mismatch → the consumer treats the user as **not visible** (fail closed).

## Consumer rule (sessions-pipeline read API; not part of the producer)
A buffered row/stage is visible only if **all** hold:
1. Response 200, exact echo of (org, workspace, user), `contract` matches, `cutover.mode == "ingest"`.
2. `head == null` **or** `head.pending == false`.
3. The row's immutable **admission** witness (from `ingest_claim_attempts` / `ingest_lazy_ack_decisions` via the stage's original `claim_token`; GC kept-row mapping must preserve the original admission, never a replacement stage) satisfies:
   - `head == null` → admission epoch == `cutover.epoch` and admission version == `"0"`;
   - `head != null` → admission epoch == `head.epoch` **and** admission version == `head.version` (no control of any kind since admission).
   - Absent/NULL admission witness → hidden.
4. Checked **after** the S3 fetch and before any byte is emitted (no streaming before the gate), for every buffered list row, metadata and turns response.

Consequence (documented, accepted): a user who issued any selection/reset after uploading loses buffered visibility of sessions admitted before that control until they are canonically published. This is a constrained visibility contract, not the full customer fix.

## Producer tests (relayhistory-cloud)
Unit + integration against a real Neon-schema Postgres: head absent; head non-pending; head pending (begin committed, complete not run); after 503 fence failure (pending stays true); echo/scope correctness; non-binding request refused; timeout classification; decimal-string encoding of values > 2^53.
