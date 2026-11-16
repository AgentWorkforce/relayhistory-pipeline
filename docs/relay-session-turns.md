# Relay session turns off Neon (the last live Neon writer)

`POST /v1/sessions/:id/turns` (relayhistory-cloud `routes/turns.ts`, called by the agent-relay
`@agent-relay/session` SessionClient) is the last route that still INSERTs into Neon
(`sessions.accept_conversation_turns` → `sessions.conversation_turns`). This change lets
relayhistory send the same turns to this ingest instead, as `conversation_turn` records in the
session's S3 stream, through the normal buffered path.

## Pieces
| Repo | What |
|---|---|
| sessions-pipeline `src/session-ingest/turns.ts` | The record shape and `parseTurnBatch` (strict; tenant-bound; record id derived from the row key). |
| sessions-pipeline `src/session-ingest/app.ts` | `POST /v1/turns/batches`: same signed binding identity, `rth:sync`, custody (relayhistory envelope, shadow, or frozen-supabase), claim, buffer stage and receipt as `/v1/delivery/batches`. `conversation_turn` is not a public `DeliveryKind`: the public route still answers 422 `unsupported_evidence` for it. |
| relayhistory-cloud `lib/turns-forward.ts` | `TURNS_WRITE_PATH` = unset/`neon` (default, today) \| `dual` \| `forward`; `TURNS_FORWARD_ORGS` canary allowlist. |

## Record shape (v1)
- `origin_id` `relayhistory:conversation_turns:v1` (one synthetic origin; Neon delivery has never seen it, so it has no legacy receipt or S3 marker).
- `kind` `conversation_turn`, `operation` `upsert`, `source` = the turn's resolved Neon source, `session_id` = the path id.
- `record_id` = `"turn:" + sha256hex(JSON.stringify([workspace_id, source, session_id, turn_index]))`, with `workspace_id` = the token's workspace or `default` (Neon's key). Pinned vector: `("default","claude","s-1",0)` → `turn:561b1d69…13ebb` in both repos. One record per Neon row key, so a re-POST of an index replaces it.
- `revision` = relayhistory's strictly increasing microsecond clock; a later write wins (the projection fold and the ledger's stale filter).
- `payload` = the Neon row: `id, org_id, workspace_id, source, session_id, turn_index, session_owner, role, content, actor_name, actor_role, metadata_json, ts, tagged`. No `ts_ms`: a Luhn-valid 13-digit number would be nulled by the delivery card scrub.
- Text (`content`, `session_owner`, `actor_name`) travels as the client sent it and is scrubbed once here by the same scrubber Neon's path uses (`packages/pipeline/src/lib/scrub.ts` is byte-identical to relayhistory's `lib/scrub.ts`). `source` and `metadata_json` are Neon's scrubbed values (the source is resolved from them). Known, deliberate differences from Neon: metadata keys `raw`, `raw_json`, `raw_path`, `raw_locator` are dropped (the delivery redaction barrier); a single turn too large for a 1.4 MB batch travels pre-scrubbed and is re-bounded, so text past the 262,144-character scrub limit carries a different truncation note.
- The projection ignores the kind (no transcript events, no catalog rows: `unprojected`).

## Modes (relayhistory)
| Mode | Neon | Ingest | Response | Reads |
|---|---|---|---|---|
| `neon` (default) | write | none | Neon's | Neon |
| `dual` | write, authoritative | background (`waitUntil`) forward of what Neon holds after the write committed (read back by key: commit order, Neon's ids; fenced or erased keys never copied); a re-read after the ack logs `erased_during_forward` | Neon's, unchanged | Neon |
| `forward` | none; reads Neon's erasure fence and drops what `accept_conversation_turns` would | required; one batch per ≤500 rows | 200 only after every batch's durable receipt (batch id, every revision id), else 503 `turns_unavailable`; refuses (`turn_fence_unavailable`) without a readable fence | Neon (stale!) |

Under relayhistory custody (`DELIVERY_CUSTODY_SOURCE_MODE` unset), the forward still READS Neon (custody witness, cutover state), like a delivery batch. Only `frozen-supabase` takes Neon off it entirely.

## Activation gates
The code paths for all six gates are present, but this document does not authorize a production
migration, export, publication run, or flag change. Those remain separate operator actions with
Khaliq's explicit approval. In order:

1. **Read-your-writes.** The canonical Cloud GET calls the read Worker before any Neon client is
   created. With owner-buffered exact-session reads enabled, a source-free SessionClient GET uses
   `buffered_turn_sources_for` and the existing twice-gated stage reader. It does not depend on a
   directory row, so a just-acknowledged turn is visible before publication; when a canonical
   pointer already exists, the newer buffered revision is merged over that image by the same
   live-revision rule. Cloud refuses a non-empty `forward` write before Neon or ingest unless
   `SESSION_READ_BACKEND=canonical`, so a misordered writer flip cannot acknowledge an invisible
   turn.
2. **Erasure fence off Neon.** Migration `20261105062020_turns_forward_gates.sql` adds the permanent
   logical turn-fence census and `ingest_turn_register`. The turns route registers its exact writers
   under a per-logical-session transaction advisory lock shared with erasure capture, then attests
   the completed registration with `X-Relay-Turn-Fence: registered-v1`; relayhistory-cloud refuses
   before posting any batch unless the independently default-off `INGEST_TURN_FENCE_REGISTER=on`
   gate installs the authenticated `GET /v1/turns/capabilities` probe, reads the
   migration-owned capability RPC, and returns that header. Each batch response repeats the attestation
   after registration and before async enqueue or claim. Registration deliberately precedes the
   downstream custody/enqueue/claim decision: a later refusal can leave a privacy-only writer row,
   but can never admit bytes without the erase race fence. Any ordinary `upload_deletions` insert fans out to the synthetic
   turns origin; registration after an older fence materializes the same deletion. Before activation,
   import every frozen Neon `conversation_turn_fences` key through `ingest_turn_fences_import` and
   compare the source and target ordered key counts/digests.
   Operator whole-user erase does not manufacture one exact-session deletion per registered writer;
   registration shares its global privacy lock, and the erase-ledger insert removes that user's
   auxiliary writer rows. The durable session fence and synthetic pointer marker contain no erased
   content and keep later directory retries hidden.
3. **Custody.** Cloud forward is Neon-free only in `frozen-supabase`. `supabase-authority` continues
   to refuse all non-empty turn writes and must not be used for this migration. Flip custody first,
   under its own runbook and approval.
4. **Erasure during a forward.** Fence materialization bumps the normal turn-origin fence version,
   writes `upload_deletions`, deletes stored revisions/ownership, and therefore drives the existing
   buffered GC and publication-retire paths. A forward racing an erasure is either dropped at claim/
   commit or retired and purged; there is no log-driven manual privacy gap.
5. **Publication.** `aa_session_directory_hide_turn_only` tombstones a directory projection whose
   live pointers are exclusively registered turns-origin pointers. The canonical pointer stays live
   for GET turns. A normal pointer makes the predicate false and the ordinary projector restores the
   directory row.
6. **Backfill.** `migration:legacy-export` is the bounded, resumable one-shot exporter/verifier for
   `conversation_turns`, links and outcomes. Its manifest is accepted only on the dedicated legacy
   machine. The read merge remains live-over-legacy per
   `(workspace, source, session, turn_index)`. Run `--verify` successfully after the final export;
   once forward begins, never return to `neon` without re-exporting the affected scopes.

Activation is allowed only after the migration is applied, the legacy fence import and legacy export
both verify equal, `frozen-supabase` is already healthy, owner-buffered exact-session reads and
canonical reads are healthy for the canary, and publication/GC have no backlog. `TURNS_WRITE_PATH`
stays unset/`neon` while these code changes are reviewed.

## Rollback
Unset `TURNS_WRITE_PATH` (live in seconds). Records already forwarded stay canonical. Neon is
complete only for a preceding `dual` interval; after any direct `forward` interval, returning to
Neon would split authority and is forbidden until the affected scopes are re-exported and verified.
