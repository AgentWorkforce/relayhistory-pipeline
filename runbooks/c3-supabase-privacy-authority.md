# C3/E3: Supabase as the privacy authority (zero-Neon checklist)

Lead-operated only. Every step below except the READ-ONLY ones is a production action, and deploys, flags,
and migration applies each still need Khaliq's separate, explicit OK. Nothing here has been run against production.

## What changes

Today Neon is the privacy authority for `POST /v1/delivery/sessions/{selection,reset}` in every relayhistory
mode, `frozen-supabase` included: the Supabase mirror only wraps Neon's begin/apply/complete. This adds a
fourth, default-off mode in which Neon is not called at all:

| Layer | Change | Default |
|---|---|---|
| Supabase | migration `20261105041000_ingest_privacy_authority` (slot 041000): authority row `fenced`/`active`, owner fence/activate/release, `ingest_privacy_authority_begin/complete`, statement guard on heads and frozen tables, legacy mirror begin/complete refuse once the authority leaves Neon | no row = Neon authority, nothing changes |
| Supabase | `scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql`: `CREATE INDEX CONCURRENTLY` on frozen records `(org, ws, user, source, session)` | non-transactional post-apply operator job (no slot: the migrator's transaction would fail it with 25001); explicit apply OK only; activation refuses without it |
| Supabase | migration `20261105040000_ingest_lazy_source_tables` (slot 040000, **independent, needed earlier**): lazy-current adoption checks ignore source-side tables | fixes a live blocker for mirror and frozen-supabase (below) |
| Ingest Worker | `INGEST_PRIVACY_AUTHORITY=supabase` (needs `INGEST_CUSTODY_SOURCE=frozen-supabase`): `/v1/custody/authority/{begin,complete}` open, legacy mirror routes and every relayhistory-built (Neon) custody envelope refused | `neon` |
| relayhistory-api | `DELIVERY_CUSTODY_SOURCE_MODE=supabase-authority`: uploads as frozen-supabase; selection/reset = authority begin, ordered fence with the ingest-built frozen witness, authority complete | unset |
| Eraser (E3) | `S3X_PRIVACY_AUTHORITY=supabase`: reads `ingest_frozen_source_deletions`, no Neon presence sweep, erases nothing unless the authority is `active`; credentials only on stdin | `neon` |

The authority begin applies Neon's own `set_session_uploads` / `reset_uploaded_sessions` semantics to the
frozen tables (disable: insert the deletion and drop that user's frozen records of the session; re-enable:
remove the deletion; reset: fence every owned frozen session, record `(request_id, count)`, replay the count),
in the same transaction as the head. So the frozen witness keeps answering what the Neon witness would have,
and the eraser reads the frozen deletions exactly where it read Neon's `deleted_upload_sessions`.

## Preconditions (all READ-ONLY checks; STOP if any fails)

1. Migrations 040000 and 041000 were applied in a reviewed window, and the index job
   (`scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql`, run outside any transaction under an explicit
   apply OK) finished with a valid index.
2. The frozen copy is sealed (`ingest_frozen_source_completeness` row matches the live policy) and the ingest runs
   `INGEST_CUSTODY_SOURCE=frozen-supabase` with a clean shadow history (C1 done).
3. relayhistory-api is on `mirror` or `frozen-supabase` at **100%** of isolates (`wrangler deployments list`),
   for at least the settle time. Any `relayhistory`-mode isolate writes Neon without the mirror and defeats the fence.
4. **History reads and the turns writer no longer touch Neon history.** Under this authority Neon's transcript tables are
   never purged by a selection or reset (Neon is not written). Every RHC read route that still reads Neon `sessions`
   (checklist R1-R12, D1/D2) must be off Neon or retired first, or a deleted session stays readable there until Neon
   is dropped. This is the main stop point; do not activate on the Friday path unless it holds.
5. The eraser host is ready to restart with stdin credentials (below); the old `NEON_URL_FILE` invocation refuses on the new code.

## Steps

`OP` = `bun run scripts/privacy-authority-handoff.ts` from a clean worktree at the reviewed head. Credentials: one
JSON object on stdin, piped from 1Password, never a file: `{"neonUrl": <Neon owner URL, used read-only>, "supabaseUrl": <Supabase owner URL>}`.
`EPOCH` = the live `DELIVERY_CUTOVER` epoch.

**Session affinity (required).** Both URLs must be dedicated direct or session-mode connections, never a transaction
pooler: `sync-owned` holds a session advisory lock and a single Neon cursor, and Neon reads rely on a session-level
`default_transaction_read_only`. The CLI refuses known transaction-pooler inputs before connecting (Supabase port
`6543`, any `pgbouncer=true`, a Neon `-pooler.` host). That is a blocklist, not a proof: a URL's text cannot show
that it is session-mode, so use the Supabase direct host (`db.<ref>.supabase.co:5432`) or the session pooler on
`5432`, and the Neon unpooled endpoint. As a runtime backstop `sync-owned` pins its backend by pid. It takes the lock
only on the backend that proved it does not already hold it, checks the pid in every page transaction before
writing, and unlocks only on that backend. Any mismatch stops with `sync_session_reassigned`. If that happens, the
lock may stay held on the original backend until that session ends: rerun on a dedicated connection. Never run two
`sync-owned` processes at once; the second refuses with `sync_owned_in_progress`.

1. **Census (READ-ONLY):** `<creds> | $OP census --epoch $EPOCH`. Expect `supabase.authority: "neon"`,
   `neon.counts.pending: 0`. Record `neon.fingerprint`. `equal` may be false (the frozen deletions drift from Neon
   after the seal; activation reconciles exactly). STOP if `neon.counts` are implausible or the call refuses.
2. **Fence** (Supabase write): `<creds> | $OP fence --epoch $EPOCH`. From now on selection/reset answer 503
   (the mirror begin refuses before Neon). Uploads are unaffected. Rollback: `$OP release --epoch $EPOCH`.
3. **Sync the owned-session copy** (Supabase write, Neon read-only): `<creds> | $OP sync-owned --epoch $EPOCH`. It streams
   Neon's owned identities (exactly what Neon's reset fences) and makes `ingest_frozen_source_owned_sessions` that exact
   set. Rerun until `census` shows `ownedEqual: true`. A first pass may run before the fence to shorten it; the final pass
   must run while fenced. Neon cost: one sorted scan of `uploaded_support_identities` (row count unmeasured; time it).
4. **Wait at least the settle time (>= 120 s)**, then census again: `supabase.pendingHeads` and
   `neon.counts.pending` must be 0. If a pending head persists (a crashed in-flight control), STOP: release,
   let the user's retry complete it, then start again at step 1.
5. **Activate** (Supabase write): `<creds> | $OP activate --epoch $EPOCH --expect-neon <fingerprint from step 4> --settle-seconds 120`.
   It reads Neon twice (>= settle apart), refuses on any change or pending head, then in ONE transaction makes the
   Supabase heads/deletions/resets exactly that snapshot, checks every head against the target's control receipts,
   and activates. It also refuses unless the owned-session copy's digest equals Neon's (`owned sessions differ from neon`). Expect `m: "activated"`, `neonStableAfter: true`.
   - `STOP_neon_changed_after_activation`: a writer bypassed the fence. Run `$OP release` at once (allowed while no
     authority control happened) and find the writer.
   - Any refusal: nothing was written; read `reason`, fix, restart at step 1.
6. **Flip the ingest, then relayhistory** (deploys/flags; Khaliq's OK): ingest `privacyAuthority: "supabase"`
   (`INGEST_PRIVACY_AUTHORITY=supabase`), then relayhistory-api `DELIVERY_CUSTODY_SOURCE_MODE=supabase-authority`.
   Between activation and the flip, controls stay 503 (fail closed). Old mixed isolates during rollout also 503.
7. **Verify (READ-ONLY):** one synthetic selection round trip on a test account; census shows `authority: "active"`;
   relayhistory selection/reset Neon statements in `pg_stat_statements` delta 0; upload 2xx unchanged.
8. **Eraser:** restart in `S3X_PRIVACY_AUTHORITY=supabase` with stdin credentials
   (`{"databaseUrl": <Supabase session/direct URL>, "eraser": {...}}`, see `runbooks/neon-s3-sync.md`). Dry run first
   (no `--execute`); expect `authority: "supabase"` sweep lines and no `sweep refused`.

## Rollback

- Before step 5: `$OP release`. Nothing diverged.
- After step 5 but before any authority control: `$OP release` (it verifies the Supabase fingerprint is still the
  activation fingerprint), then revert the flags. Neon is still exactly equal.
- **After the first authority control the switch is one-way.** `release` refuses; Neon's heads and privacy are behind.
  Reverting relayhistory to `mirror`/`frozen-supabase` only makes controls 503 (fail closed, no divergence).
  Going back to Neon would need a separately reviewed Neon write of the Supabase heads and privacy.

## Migration integration (done)

- 040000 and 041000 are registered lanes (`scripts/sessions-function-boundary/lanes/{lazy-source-tables,privacy-authority}.ts`):
  the generator derives the ACL manifest, boundary SECDEF set, verifiers and both checksum-bound, later-migration-guarded
  rollbacks (`supabase/rollbacks/2026110504{0,1}000_*.down.sql`). 041000's rollback also refuses while an authority row exists.
- `ingest_privacy_authority_{begin,complete}` are the only new ingest-callable (SECURITY DEFINER) functions; the rest are owner-only.
- The index stays a separate non-transactional operator job.

## Reset coverage: what Neon's reset does, and what covers it here (no narrowing)

Neon `reset_uploaded_sessions` (0039) runs `set_session_uploads(..., false)` for every
`owned_uploaded_session_identities(org, ws, user)` (0038): the user's `uploaded_support_identities` with a session,
plus session ids recovered from null-session delete tombstones through `neighborhood_claims`. It then records
`session_resets(request_id, count)`. Per session, `set_session_uploads(false)` writes:

| Neon effect | Under the Supabase authority |
|---|---|
| `deleted_upload_sessions` insert | `ingest_frozen_source_deletions` insert for every owned session (owned copy UNION the user's frozen records) and, through the fence, target `upload_deletions` |
| `delivery_records` tombstone + delete | `ingest_frozen_source_records` delete (the witness answers "no current row", as Neon's would) |
| `uploaded_support_identities` delete | `ingest_frozen_source_owned_sessions` delete |
| `session_resets` insert (count = distinct owned (source, session)) | `ingest_frozen_source_resets` insert with the same count; a repeated request id replays it |
| target ingest data (post-cutover uploads) | unchanged: `ingest_apply_reset` through the ordered fence |
| S3 copies (P5a exports, live `srv_` objects) | the eraser in `supabase` mode erases every (org, session) in `ingest_frozen_source_deletions` |
| `daily_digests` delete (workspace) | `sessions.daily_digest_narratives` (F4, migration 045000) purged in the head transaction through `purge_daily_digest_narratives(org, ws)` on every disable and reset. Guarded: a no-op until 045000 is applied |
| SP v2 analysis results (046000 `session_analysis_jobs_v2`) | forgotten by 046000's trigger when a `session_index`/`session_directory` row is tombstoned. The ordered fence's target apply tombstones those rows only for sessions with an ingest publication head (post-cutover uploads). **Flag: Neon-era (exported) sessions' pointers are not tombstoned on this path, so their v2 analysis rows are not forgotten.** That needs a pointer-tombstone owner before activation. |
| Neon-resident derived data (projected events, rollups, catalogs, claims/edges, patterns, memory jobs, analysis requests/jobs/caches, conversation turns) | **not written** (Neon is not written). Contract: before activation, nothing may read or write these for history (precondition 4, and below); they are destroyed when Neon `sessions` is dropped. |
| `conversation_turn_fences` insert (blocks re-ingest of the session's turns) | **contract**: turns must be written through the ingest (`TURNS_WRITE_PATH=forward`, SP #241 live) with an ingest-side fence on `upload_deletions` before activation. Otherwise `/v1/sessions/:id/turns` could write a deleted session's turns into Neon. |

Owned set completeness is enforced, not assumed: `sync-owned` copies Neon's exact set, and activation refuses unless the
Neon-computed digest equals the Supabase copy's. From then on, reset and selection maintain the copy exactly as Neon
maintains `uploaded_support_identities`.

## Known gaps

- `sync-owned`'s Neon scan cost is unmeasured (time it before the fenced pass).
- Neon transcript data of deleted sessions stays in Neon until Neon `sessions` is dropped (precondition 4).
