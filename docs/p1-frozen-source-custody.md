# P1: frozen-source custody (Neon off the upload path)

Design and evidence: `~/relay-briefs/migration-fast-20261103/p1-custody-supabase-design-20261105.md`.

## What it is
Since the 2026-11-05 cutover (relayhistory 0078 phase `active`), Neon's legacy delivery tables are frozen. Migration `20261105010000_frozen_source_custody.sql` adds:
- **A metadata-only copy** of the witness inputs (`ingest_frozen_source_records`, `_deletions`, `_resets`), owner-only. A per-org seal and a completeness seal bind it to the live policy's source identity, epoch and privacy baseline.
- **`ingest_source_control_heads`**, a Supabase-first mirror of Neon's privacy-control heads. It uses the same digest/version/PRE algorithm as `begin/complete_delivery_cutover_control_v1`.
- **`ingest_frozen_source_witness(jsonb)`.** It returns the SAME `{witness, sourceDigest}` packet relayhistory builds against Neon. Packet-shaping orders use `COLLATE "C"`, because Neon is C.UTF-8 and Supabase is ICU en-US. It refuses (55000) while unsealed, while a batch's user has a pending control, or on any invalid row.
- **`ingest_source_control_begin/complete(jsonb)`** for relayhistory: begin here BEFORE Neon begins, complete here AFTER Neon completes.

The ingest Worker reads `INGEST_CUSTODY_SOURCE` (stack input `sessionIngest.custodySource`):
- unset/`relayhistory` (today): only the relayhistory Neon witness;
- `shadow`: also builds the frozen witness and logs `custody.shadow` match/mismatch with the differing top-level fields. The answer never changes;
- `frozen-supabase`: when relayhistory sends no witness, the Worker builds it. relayhistory then stops calling Neon on uploads.

## The single generation window (all through bulls-migrator, one operator)
The new migration changes `generation()`. That generation is pinned in the ingest Worker (`INGEST_EXPECT_GENERATION`), the buffered GC (`INGEST_GC_EXPECT_GENERATION`), `ingest_lazy_policy.generation` and every `ingest_buffer_admission.schema_generation`. Uploads refuse until all of them match. In one window:
1. **Owner-apply** the release (schema-release owner-apply path). Readiness goes PASS for the new generation G2. From here uploads refuse, generation not ready, until step 3.
2. **Redeploy** `96bulls-session-ingest` with `INGEST_EXPECT_GENERATION=G2` (private-ingest operator) AND the buffered GC Lambda with `INGEST_GC_EXPECT_GENERATION=G2`.
3. **Rebind**: `bun scripts/ingest-generation-rebind.ts --from G1 --to G2` (dry run, then `--execute`). That's one transaction under the migrator lock: the policy via `ingest_lazy_policy_set` (identity unchanged), then every admission row. It's idempotent.
4. **Verify** uploads reopen (2xx rate back), and no `generation not ready`.

The whole sequence is rehearsed in `frozen-custody.pglite.test.ts` ("rehearsed generation window").

## Then
1. **Copy**, outside the window. Uploads keep working because frozen mode is not yet on:
   - `bun scripts/frozen-source-copy.ts --plan`;
   - `--copy --concurrency 2`, throttled and resumable;
   - `--seal`, which verifies every org with a count + order-independent digest on both sides, plus the privacy rows and control heads, then writes the seals.
2. **Shadow**: deploy the ingest Worker with `custodySource: shadow`. Require `custody.shadow` outcome `match` on live batches and controls before going further.
   Use the CI-only `step-0-shadow` profile in [private ingest production](../runbooks/private-ingest-production.md): a reviewed tracked-profile change, a fresh exact-SHA plan, deployment, and active-version readback. Keep async admission `drain` and the synchronous frozen-receipt bridge absent. A successful mode readback is not the shadow parity proof, and no copy/seal prerequisite is waived.
3. **Mirror** (relayhistory PR): call `/v1/custody/control/begin` before Neon begin, require an equal `{version,digest}`, and call `/v1/custody/control/complete` after Neon complete. This must be live before any frozen flip. Shadow mismatches on `sourceControlVersion`/`pending` mean it isn't.
4. **Flip**: deploy the ingest Worker with `custodySource: frozen-supabase`, then relayhistory `DELIVERY_CUSTODY_SOURCE_MODE=frozen-supabase` (skip the Neon witness/pending/status calls on batches).
5. **Prove**: the runtime role's Neon upload-path statements reach zero (pg_stat_statements delta), and the upload 2xx rate is unchanged or better.

**Rollback**: unset the relayhistory flag. It goes back to sending the Neon witness, which the Worker accepts in every mode.
