# R2 control-head contract v5 (supersedes v4 a10f2990 transport; v4 semantics retained) — 2026-11-08

Pinned by pipeline-lead. Producer: relayhistory-cloud PR #328 (`SessionReadPrivacyControls` named entrypoint). Consumer: sessions-pipeline read API (PR B) + migration 043700 (#328 pipeline). Reviewers: neonexit-review-dogpatch, codex-relay-desktop-find-prior.

## Transport (matches relayhistory-cloud #328 @ e9161963)
- Cloudflare **named-entrypoint service binding only** (`SessionReadPrivacyControls`); the read Worker's binding pins that entrypoint. The public default fetch must never dispatch it (real public-request and default-entrypoint negatives required).
- `GET /v1/session-read/control-head`, **no query string**. Any other method → 405, any other path/query → 404.
- Identity carriage: request headers `x-relayhistory-org-id`, `x-relayhistory-workspace-id`, `x-relayhistory-user-id`, set by the read Worker **only** from the verified RelayAuth identity (never from client input). Missing/empty/over-128 → 400.
- Consumer deadline: **2 s** total; timeout = not visible.

## Response 200 (exact keys, no extras)
```json
{
  "tenant": { "orgId": "<echo>", "workspaceId": "<echo>", "userId": "<echo>" },
  "controlEpoch": "<hex64>",
  "privacyBaselineSha256": "<hex64>",
  "sourceControlVersion": "<decimal string 0..2^63-1>",
  "sourceControlDigest": "<hex64>" | null,
  "sourceControlKind": "selection" | "reset" | null,
  "pending": true | false
}
```
- `controlEpoch` / `privacyBaselineSha256`: the ACTIVE cutover epoch and privacy baseline read from the database in the same snapshot as the head; the producer refuses (non-200) if the head row's epoch or baseline differs from the active cutover (never reported as "no head").
- No head for this exact tenant ⇒ `sourceControlVersion: "0"`, `sourceControlDigest: null`, `sourceControlKind: null`. Version > 0 ⇒ digest is hex64 and kind ∈ {selection, reset}; anything else ⇒ non-200.
- `cache-control: no-store`.
- Errors: non-200 with `{ "error": "<code>" }`; consumer treats any non-200, shape mismatch, tenant echo mismatch, or timeout as **not visible**.

## Consumer rule (read API; unchanged from v4)
A buffered row/stage is visible only if ALL hold, evaluated **after** the S3 fetch and before any byte is emitted, on every buffered list row, metadata and turns response:
1. 200 + exact shape + exact tenant echo.
2. `pending == false`.
3. The row's ORIGINAL admission witness (043700 RPC; version = `source_control_version` on the original `ingest_claim_attempts` / `ingest_lazy_ack_decisions` row via the stage's original `claim_token`, GC kept rows keep the original claim; epoch = immutable `ingest_lazy_policy.control_epoch` verified against the claim's `source_identity_sha256`) satisfies: admission epoch == `controlEpoch` **and** admission version == `sourceControlVersion` (decimal BigInt compare). NULL/missing witness → hidden.
4. A current per-user G2 parity row exists in Supabase for this exact (org, workspace, user), bound to `controlEpoch`, `privacyBaselineSha256`, the head tuple (`sourceControlVersion`, `sourceControlDigest`, `sourceControlKind`) or explicit no-head, the full deletion/reset set digests, `target_lineage_digest` over contiguous `ingest_control_receipts` 1..V, and the current target schema generation. Missing/stale/mismatch → hidden. Default: no rows.

Head + admission equality alone is **never** sufficient; rule 4 is mandatory.

## Changes vs v4
Transport/key names follow #328; semantics unchanged. `privacyBaselineSha256` added to the response and to the G2 binding.
