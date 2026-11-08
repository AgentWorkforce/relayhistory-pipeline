# R2 control-head contract v4 (supersedes v3 / v2 dbe56a03 / v1 54ce8bc5) (Option C privacy gate) — 2026-11-08

Pinned by pipeline-lead. Producer: relayhistory-cloud (relayhistory-cloud-owner). Consumer: sessions-pipeline session read Worker (layered-sessions-pipeline). Adversarial: neonexit-review-dogpatch, codex-relay-desktop-find-prior (fixtures v3 `25bbbdfa…`, 26 cases).

## Transport
- Cloudflare **named-entrypoint service binding only**: relayhistory-api exports a distinct `WorkerEntrypoint` class (e.g. `ControlHeadEntrypoint`); the read Worker's binding `HISTORY_CONTROL_HEAD` pins `entrypoint = "ControlHeadEntrypoint"`. The public default `fetch` handler must **never** dispatch this handler; every public path for it answers 404. Header absence (`cf-connecting-ip`, hostname) is NOT a capability check. Integration negatives required: a real public request is refused; a default-entrypoint binding is refused.
- RPC method or `POST /internal/v1/privacy/control-head` on that entrypoint, JSON, body ≤ 4 KiB.
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
  "cutover": { "epoch": "<hex64>", "mode": "ingest", "status": "active" },
  "head": null | { "epoch": "<hex64>", "version": "<decimal string>", "digest": "<hex64>", "kind": "selection" | "reset", "pending": true|false },
  "observedAt": "<RFC3339 UTC>"
}
```
- Source: `cutover` from the DATABASE `delivery_cutover_state` / status (must be ACTIVE and equal the configured `DELIVERY_CUTOVER` epoch, else `503 cutover_mismatch`), and `head` from `sessions.delivery_cutover_control_heads` for exactly (org, workspace, user) — read-only, **same statement / snapshot** for both, `statement_timeout` ≤ 2 s. A non-null head whose epoch differs from the cutover epoch is answered `503 head_epoch_mismatch` (never filtered to null, never returned as visible-eligible). Any `kind` other than `selection`/`reset` → `503 unknown_control_kind` (a new kind needs a contract bump).
- `head = null` means **no control row exists for this exact (org, workspace, user)** (scope-exact; a foreign-epoch row is refused with 503, never reported as absence) (selection, reset, and any other control kind all write this head per 0078). Missing user/workspace is not an error: it is `head: null`.
- Epochs are lowercase **hex64** (Neon 0078 CHECK). Versions are **decimal strings** (consumer parses as BigInt, never JS Number).
- No caching (`cache-control: no-store`); no cross-request memoization.

## Errors
- `400 invalid_request`, `403 not_binding`, `503 neon_unavailable` (timeout/connection), `500 internal`. Body `{ "error": { "code": "<code>", "reason": "<code>" } }`. Any non-200, timeout (> 2.5 s at the caller), shape mismatch, or echo mismatch → the consumer treats the user as **not visible** (fail closed).

## Consumer rule (sessions-pipeline read API; not part of the producer)
A buffered row/stage is visible only if **all** hold:
1. Response 200, exact echo of (org, workspace, user), `contract` matches, `cutover.mode == "ingest"` and `cutover.status == "active"` (shape constants).
2. `head == null` **or** (`head.pending == false` **and** `head.epoch == cutover.epoch`).
3. The row's immutable **admission** witness satisfies the rule below. Admission VERSION = `source_control_version` on the original `ingest_claim_attempts` / `ingest_lazy_ack_decisions` row (via the stage's original `claim_token`; GC kept-row mapping must preserve the original admission, never a replacement stage). Admission EPOCH is not stored on those rows: derive it from the immutable `ingest_lazy_policy.control_epoch` (identity rotation refused at 055000:113-131) joined and verified against the claim's source identity, OR 043700 persists `source_control_epoch` on new claims/decisions — the PR must state which and test it, including GC child provenance. Rule:
   - `head == null` → admission epoch == `cutover.epoch` and admission version == `"0"`;
   - `head != null` → admission epoch == `head.epoch` **and** admission version == `head.version` (no control of any kind since admission).
   - Absent/NULL admission witness → hidden.
4. **Mandatory per-user G2 parity proof:** an owner-written Supabase row for this exact (org, workspace, user) exists whose proof is bound to: the cutover epoch; the current head tuple (epoch, version, digest) or an explicit no-head proof; the full current deletion-set and reset-set digests plus the ordered-control lineage digest (Neon heads vs contiguous Supabase `ingest_control_receipts`); and the current target schema generation. Missing, stale (head or generation moved), or mismatched → hidden. Workspace canary selection is NOT a substitute (covers pre-cutover controls that left no head). Default: no rows (nobody visible).
5. Checked **after** the S3 fetch and before any byte is emitted (no streaming before the gate), for every buffered list row, metadata and turns response.

Consequence (documented, accepted): a user who issued any selection/reset after uploading loses buffered visibility of sessions admitted before that control until they are canonically published. This is a constrained visibility contract, not the full customer fix.

## Producer tests (relayhistory-cloud)
Unit + integration against a real Neon-schema Postgres: head absent; head non-pending; head pending (begin committed, complete not run); after 503 fence failure (pending stays true); echo/scope correctness; non-binding request refused; timeout classification; decimal-string encoding of values > 2^53.
