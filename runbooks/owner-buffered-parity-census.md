# Owner-buffered parity census: source selection

The parity operator (`scripts/owner-buffered-parity-operator.ts`, Option C G1
`census`, G2 `write`, and `bulk`) compares one user's source-side privacy
state (control head, deletion set, reset set) with the target's 043700
`sessions.owner_buffered_parity_expected` digest. The script header is the
canonical usage; this page covers only where the source side is read from
(Neon-exit step 6, edge 5).

**No production census run is authorized by this change.** Adding the input
changes no running system. Any G1, G2 or bulk run, in either mode, still needs
Khaliq's explicit OK for that run, in his own words, plus the usual pinned
checkout and RAM-only credential pipe.

## `--census-source`

| Value | Source read | Stdin (exactly these keys) |
| --- | --- | --- |
| `neon` (default) | Neon `delivery_cutover_status()`, `delivery_cutover_control_heads`, `deleted_upload_sessions`, `session_resets` | `{"neonUrl":"...","supabaseUrl":"...","tenant":{"orgId":"...","workspaceId":"...","userId":"..."}}`; bulk: `{"neonUrl":"...","supabaseUrl":"..."}` |
| `frozen-supabase` | The sealed frozen copy in Supabase: `ingest_source_control_heads`, `ingest_frozen_source_deletions`, `ingest_frozen_source_resets`, with epoch and baseline taken from `ingest_frozen_source_completeness` | `{"supabaseUrl":"...","tenant":{...}}`; bulk: `{"supabaseUrl":"..."}` |

The flag is accepted in all three modes. Without it, behaviour, stdin contract
and output bytes are exactly as before. Frozen-mode receipts end with
`"censusSource":"frozen-supabase"`; Neon-mode receipts have no such field.
The tenant tuple keeps the same semantics in both modes.

In frozen mode the source is read on its own Supabase connection through the
same canonical fenced owner route as the target (the frozen tables are
owner-only), pinned `default_transaction_read_only`. Each read is a separate
`REPEATABLE READ READ ONLY` transaction. The snapshot discipline is unchanged:
source snapshot A, then the target census, then source snapshot B. Both
sources use the same digest expressions (`COLLATE "C"` order), so the same
logical rows give byte-identical digests and proofs.

## Refusals (frozen mode)

| Code | Meaning |
| --- | --- |
| `census_source_invalid` | `--census-source` is not `neon` or `frozen-supabase` (exit 64) |
| `census_source_conflict` | stdin carries a `neonUrl` key |
| `credentials_missing_key.supabaseUrl` / `.tenant` | a required key is missing |
| `frozen_source_unsealed` | no completeness seal, or the seal's source identity, epoch or baseline differs from the live `ingest_lazy_policy` |
| `privacy_authority_not_active` | `ingest_privacy_authority` is absent, `fenced`, or `active` at a different epoch than the seal |
| `frozen_source_changed_during_census` | snapshot A and snapshot B differ (a control landed during the census) |
| `frozen_source_cutover_not_active`, `frozen_source_head_shape_invalid`, `frozen_source_deletion_set_too_large`, `frozen_source_reset_set_too_large`, `frozen_source_row_missing` | the frozen counterparts of the existing `neon_*` guards |

Readiness refusals are raised on snapshot A, before the target is read. A
target that differs from the source is still reported as `mismatch` with the
same mismatch codes as Neon mode. No output contains a URL or a tenant
identifier.

Tests: `src/migration/__tests__/owner-buffered-parity-frozen.pglite.test.ts`,
`src/migration/__tests__/owner-buffered-parity-operator-census-source.test.ts`.

## CI discovery mode (roster source)

The CI workflow `.github/workflows/owner-buffered-parity.yml` has a one-time,
read-only `discovery` mode that finds the seven buffered owners without a
canonical current parity row. The full contract is in
`docs/owner-buffered-parity-ci.md`. In short:

- **Every dispatch is Khaliq's own word to the production actor.** Merging
  the mode authorizes no run.
- It is read-only. It uses the same gates, environment and single secret as
  census; the target session is fenced read-only and nothing is written. It
  reuses the bounded buffered-tenant cursor and the canonical readback
  comparison. The owner listing and the generation re-read share one
  `REPEATABLE READ READ ONLY` snapshot, while each owner's comparison runs in
  `readParityCensus`'s own read-only transactions, with the generation checked
  before, inside and after. It adds no query SQL (only the transaction, cursor
  and timeout statements around the existing cursor query).
- It runs only while the roster is empty. It refuses `roster_present` once a
  digest is listed, `generation_unstable` if the PASS generation moves
  around the snapshot, and `unexpected_scope` unless exactly 7 owners are
  missing or stale. A refusal carries counts only, never digests.
- On success the `owner-buffered-parity-discovery/v1` receipt (job summary
  and artifact) lists each owner's `tenantSha256` with `missing` or `stale`,
  plus the counts. It holds no tenant identifier.
- **The roster is a separate reviewed roster-only PR** written from that
  receipt. Discovery never edits the roster.
