# Neon-exit release profile: edge map

`SESSIONS_NEON_EXIT` (default off) is the release profile for running the
services in this repo with no Neon edge. It is a strict boolean string:

| Value | Posture |
| --- | --- |
| unset or `false` | legacy posture, behaviour unchanged |
| `true` | profile on: every Neon edge listed below refuses at startup |
| anything else | startup throws (`SESSIONS_NEON_EXIT must be unset, "true" or "false"`) |

The gate is one pure module, `src/shared/neon-exit.ts` (`neonExitProfile`,
`assertNoNeonEdge`, `NeonExitRefusal` with code `neon_exit_refused`). It takes
an env record, so a Worker's env object and a Lambda's `process.env` are read
the same way. A refusal names `SESSIONS_NEON_EXIT`, the consumer and the
offending setting, never a setting's value.

There is no Neon SDK in this repo. Every Postgres client is plain `pg` given a
URL, so a consumer is a Neon edge only through what it is configured to reach.
The profile therefore gates configuration, not hosts: there is no hostname
allow or deny list, and no host check can stand in for a refused flag.

**This profile is not a global zero-Neon guarantee.** It fails closed on the
runtime edges in this repo only. Edges on the Cloud side, in unmerged work and
in the preserved tooling below are outside it.

## Runtime consumers

| Consumer | Route / flag | Under the profile | Test that proves it | Gap |
| --- | --- | --- | --- | --- |
| Ingest replica queue (`src/session-ingest/worker.ts`, `replica.ts`) | `INGEST_REPLICA_MODE` (off / queue / required / best_effort) and the `NEON_REPLICA` queue binding | Disabled: `ingestSettings` refuses any mode other than `off`, and refuses a bound `NEON_REPLICA` even with the mode off. The Worker answers 503 `not_configured` (logged `neon_exit_refused`); the fan-out consumer retries the message later | `src/session-ingest/__tests__/neon-exit.test.ts` (poisoned queue never called, `fetch` never called) | None in this repo; see the deploy layer rows |
| Ingest custody witness (`src/session-ingest/app.ts`) | `INGEST_CUSTODY_SOURCE` (relayhistory default / shadow / frozen-supabase), `INGEST_ACCEPTANCE_MODE` (canonical default / buffered) | Partial replacement: only `frozen-supabase` with `buffered` acceptance is allowed; the witness is built from the sealed Supabase copy. `relayhistory`, `shadow` and canonical acceptance are refused | `src/session-ingest/__tests__/neon-exit.test.ts` (refusals; success, unsealed 503, 400, retried batch with the same receipt, 501 legacy route through the worker's own app builder) | Canonical acceptance has no non-Neon witness, so it is unavailable under the profile (partial replacement) |
| Ingest `/v1/delivery/recovery` | buffered acceptance, HMAC over the private body | Kept: it reads archive bytes, not a Neon connection | Existing `src/session-ingest/__tests__/deliver.test.ts` and `lazy-custody.pglite.test.ts` (recovery route) | None |
| Session-pipeline analysis cache (`src/session-pipeline/lambda.ts`, `analysis-snapshot.ts`) | `SESSION_ANALYSIS_CACHE_ENABLED=1`, secret field `analysis_source_readonly_url` | Disabled: `lambdaEnv` refuses the flag, so every invocation kind fails init before any secret lookup. With the flag off, `runtimeFromSecret` drops the URL, so the readonly pool cannot be built | `src/session-pipeline/__tests__/neon-exit.test.ts` (`pg.Pool` constructions and connects recorded; none to the poisoned URL) | No non-Neon replacement for the legacy readonly source |
| Session-pipeline semantic copy (`lambda.ts`, `src/search-features/semantic-copy-main.ts`, `semantic-copy-source-pool.ts`) | `SESSION_SEMANTIC_COPY_ENABLED=1`, same secret field | Disabled: `lambdaEnv` and `semanticCopyMain` refuse the flag before any factory, pool or credential | `src/session-pipeline/__tests__/neon-exit.test.ts` | No non-Neon replacement for the legacy readonly source |
| Session read API 501 routes (`src/session-read-api/app.ts`) | `/v1/sessions/output`, `/v1/digest/daily`, workspaces outside `READ_API_DIRECTORY_WORKSPACES`, and `/v1/sessions/titles` for workspaces outside `READ_API_BRIEF_TITLES_WORKSPACES` or before 20261105043000 is applied (docs/brief-titles.md) | Unchanged here: these answer 501 from this repo; the fallback to relayhistory and Neon happens on the Cloud side | None in this repo | Gap: not disableable from this repo; owned by the Cloud-side cutover |
| authority-v1 ingest (S3 upload lanes C/E/B, unmerged, not on main) | n/a on this base | Integration gap: not on this base. Per the Lane E owner, the selected authority-v1 upload path constructs no pg client and makes no relayhistory, Neon or NEON_REPLICA call: fetch returns from the authority handler before ingestSettings, pg.Client, the ledger, custody and replica, and the queue consumer retries and returns before the legacy settings/client. Unverified here; must be re-checked against this gate when those lanes land. | None | Integration, coordinated with the lane E inbox worker |
| Auth token lookup | `rth_at_` / `rth_st_` tokens | Not ported: the upstream Neon lookup stays upstream | None | Gap: the token lookup is not in this repo |
| Deploy layer, ingest (`stacks/cloudflare/session-ingest.ts`, `scripts/private-ingest-operator.ts`) | Stack input `sessionIngest.neonExit: "on"` (omit for off; any other value is refused) renders `SESSIONS_NEON_EXIT=true` | Refused at the deploy layer: `assertSessionIngestInputs` refuses, by name, `replicaMode` other than `off`, any `neonReplicaQueue`, acceptance other than `buffered` and `custodySource` other than `frozen-supabase` (the same edges `ingestSettings` refuses, so a deploy never ships a Worker that refuses every request). `sessionIngestWorkerProps` throws if `NEON_REPLICA` would still be rendered. The retained-plan operator refuses `native_neon_exit_replica` when a `NEON_REPLICA` binding would remain on the Worker (create, update or noop); a `delete` of a stale `NEON_REPLICA` is not that refusal, but the reviewed delete allowlist admits no such unbind yet, so it still fails closed (`native_binding_target`). Omitted: vars and bindings byte-identical to before | `stacks/cloudflare/__tests__/session-ingest.test.ts` (including the stack/runtime cross-check through `ingestSettings`), `stacks/cloudflare/__tests__/private-ingest-operator.test.ts` (`neon-exit` plan proof) | The production release contract (`src/session-ingest/private-ingest-release.ts`) does not admit the input yet (`private_ingest_profile_extra`); engaging it in production is a separately reviewed change |
| Deploy layer, pipeline (`stacks/aws/session-pipeline.ts`) | Stack input `transcriptStore.sessionPipeline.neonExit: true` (omit for off; only `true` is accepted) renders `SESSIONS_NEON_EXIT=true` | Refused at the deploy layer together with `analysisCache: true` or `semanticCopy.enabled: true` (the flags `lambdaEnv` refuses). Omitted: the Lambda env is byte-identical to before | `stacks/aws/__tests__/session-pipeline.test.ts` (including the stack/runtime cross-check through `lambdaEnv`) | None in this repo |

## Preserved tooling (out of scope)

These read or write Neon on purpose for the migration and are not gated by the
profile. They are operator tooling, not service runtime:

- `src/migration/**`: `neon-s3-sync`, `neon-s3-erasure`, `relayhistory-*`, `target-guard`
- `src/hoopdb/frozen-control-head-repair.ts`
- `src/hoopdb/session-directory-title-backfill.ts`
- `src/session-ingest/seed.ts` (`seedFences`)
- `src/history-db/migrate.ts`
- `scripts/`
- `supabase/`
