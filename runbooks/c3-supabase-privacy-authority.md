# C3/E3: Supabase as the privacy authority (zero-Neon checklist)

Lead-operated only. Every step below except the READ-ONLY ones is a production action, and deploys, flags,
and migration applies each still need Khaliq's separate, explicit OK. Nothing here has been run against production.

## What changes

Today Neon is the privacy authority for `POST /v1/delivery/sessions/{selection,reset}` in every relayhistory
mode, `frozen-supabase` included: the Supabase mirror only wraps Neon's begin/apply/complete. This adds a
fourth, default-off mode in which Neon is not called at all:

| Layer | Change | Default |
|---|---|---|
| Supabase | `supabase/pending/ingest_privacy_authority.sql` (slot TBD): authority row `fenced`/`active`, owner fence/activate/release, `ingest_privacy_authority_begin/complete`, statement guard on heads and frozen tables, legacy mirror begin/complete refuse once the authority leaves Neon | no row = Neon authority, nothing changes |
| Supabase | `supabase/pending/ingest_privacy_authority_index.sql`: `CREATE INDEX CONCURRENTLY` on frozen records `(org, ws, user, source, session)` | post-apply job; activation refuses without it |
| Supabase | `supabase/pending/ingest_lazy_source_tables.sql` (slot TBD, **independent, needed earlier**): lazy-current adoption checks ignore source-side tables | fixes a live blocker for mirror and frozen-supabase (below) |
| Ingest Worker | `INGEST_PRIVACY_AUTHORITY=supabase` (needs `INGEST_CUSTODY_SOURCE=frozen-supabase`): `/v1/custody/authority/{begin,complete}` open, legacy mirror routes and every relayhistory-built (Neon) custody envelope refused | `neon` |
| relayhistory-api | `DELIVERY_CUSTODY_SOURCE_MODE=supabase-authority`: uploads as frozen-supabase; selection/reset = authority begin, ordered fence with the ingest-built frozen witness, authority complete | unset |
| Eraser (E3) | `S3X_PRIVACY_AUTHORITY=supabase`: reads `ingest_frozen_source_deletions`, no Neon presence sweep, erases nothing unless the authority is `active`; credentials only on stdin | `neon` |

The authority begin applies Neon's own `set_session_uploads` / `reset_uploaded_sessions` semantics to the
frozen tables (disable: insert the deletion and drop that user's frozen records of the session; re-enable:
remove the deletion; reset: fence every owned frozen session, record `(request_id, count)`, replay the count),
in the same transaction as the head. So the frozen witness keeps answering what the Neon witness would have,
and the eraser reads the frozen deletions exactly where it read Neon's `deleted_upload_sessions`.

## Preconditions (all READ-ONLY checks; STOP if any fails)

1. Both pending SQL files have registry slots and were applied in a reviewed window (with the function-boundary
   manifest updated; see "Migration integration"), and the index job finished with a valid index.
2. The frozen copy is sealed (`ingest_frozen_source_completeness` row matches the live policy) and the ingest runs
   `INGEST_CUSTODY_SOURCE=frozen-supabase` with a clean shadow history (C1 done).
3. relayhistory-api is on `mirror` or `frozen-supabase` at **100%** of isolates (`wrangler deployments list`),
   for at least the settle time. Any `relayhistory`-mode isolate writes Neon without the mirror and defeats the fence.
4. **History reads no longer serve deleted sessions from Neon.** Under this authority Neon's transcript tables are
   never purged by a selection or reset (Neon is not written). Every RHC read route that still reads Neon `sessions`
   (checklist R1-R12, D1/D2) must be off Neon or retired first, or a deleted session stays readable there until Neon
   is dropped. This is the main stop point; do not activate on the Friday path unless it holds.
5. The eraser host is ready to restart with stdin credentials (below); the old `NEON_URL_FILE` invocation refuses on the new code.

## Steps

`OP` = `bun run scripts/privacy-authority-handoff.ts` from a clean worktree at the reviewed head. Credentials: one
JSON object on stdin, piped from 1Password, never a file: `{"neonUrl": <Neon owner URL, used read-only>, "supabaseUrl": <Supabase owner URL>}`.
`EPOCH` = the live `DELIVERY_CUTOVER` epoch.

1. **Census (READ-ONLY):** `<creds> | $OP census --epoch $EPOCH`. Expect `supabase.authority: "neon"`,
   `neon.counts.pending: 0`. Record `neon.fingerprint`. `equal` may be false (the frozen deletions drift from Neon
   after the seal; activation reconciles exactly). STOP if `neon.counts` are implausible or the call refuses.
2. **Fence** (Supabase write): `<creds> | $OP fence --epoch $EPOCH`. From now on selection/reset answer 503
   (the mirror begin refuses before Neon). Uploads are unaffected. Rollback: `$OP release --epoch $EPOCH`.
3. **Wait at least the settle time (>= 120 s)**, then census again: `supabase.pendingHeads` and
   `neon.counts.pending` must be 0. If a pending head persists (a crashed in-flight control), STOP: release,
   let the user's retry complete it, then start again at step 1.
4. **Activate** (Supabase write): `<creds> | $OP activate --epoch $EPOCH --expect-neon <fingerprint from step 3> --settle-seconds 120`.
   It reads Neon twice (>= settle apart), refuses on any change or pending head, then in ONE transaction makes the
   Supabase heads/deletions/resets exactly that snapshot, checks every head against the target's control receipts,
   and activates. Expect `m: "activated"`, `neonStableAfter: true`.
   - `STOP_neon_changed_after_activation`: a writer bypassed the fence. Run `$OP release` at once (allowed while no
     authority control happened) and find the writer.
   - Any refusal: nothing was written; read `reason`, fix, restart at step 1.
5. **Flip the ingest, then relayhistory** (deploys/flags; Khaliq's OK): ingest `privacyAuthority: "supabase"`
   (`INGEST_PRIVACY_AUTHORITY=supabase`), then relayhistory-api `DELIVERY_CUSTODY_SOURCE_MODE=supabase-authority`.
   Between activation and the flip, controls stay 503 (fail closed). Old mixed isolates during rollout also 503.
6. **Verify (READ-ONLY):** one synthetic selection round trip on a test account; census shows `authority: "active"`;
   relayhistory selection/reset Neon statements in `pg_stat_statements` delta 0; upload 2xx unchanged.
7. **Eraser:** restart in `S3X_PRIVACY_AUTHORITY=supabase` with stdin credentials
   (`{"databaseUrl": <Supabase session/direct URL>, "eraser": {...}}`, see `runbooks/neon-s3-sync.md`). Dry run first
   (no `--execute`); expect `authority: "supabase"` sweep lines and no `sweep refused`.

## Rollback

- Before step 4: `$OP release`. Nothing diverged.
- After step 4 but before any authority control: `$OP release` (it verifies the Supabase fingerprint is still the
  activation fingerprint), then revert the flags. Neon is still exactly equal.
- **After the first authority control the switch is one-way.** `release` refuses; Neon's heads and privacy are behind.
  Reverting relayhistory to `mirror`/`frozen-supabase` only makes controls 503 (fail closed, no divergence).
  Going back to Neon would need a separately reviewed Neon write of the Supabase heads and privacy.

## Migration integration (when slots are assigned)

- Move `ingest_lazy_source_tables.sql` and `ingest_privacy_authority.sql` into `supabase/migrations/<slot>_...`
  and the `.down.sql` files into `supabase/rollbacks/`, adding the usual later-migration guard and ledger cleanup.
- Register the new functions in `src/hoopdb/sessions-function-acl.json` and the boundary wrapper (pattern:
  `scripts/sync-ingest-frozen-boundary.ts`): owner-only `ingest_privacy_authority_{lock,guard,packet,state,fence,activate,release,apply}`;
  `ingest_privacy_authority_{begin,complete}` = owner + `bulls96_session_ingest` EXECUTE, SECURITY DEFINER.
- The index file stays a separate non-transactional post-apply job.

## Known gaps

- Neon's reset covers sessions known only through `uploaded_support_identities` (tombstones without a session id);
  the frozen copy has no such table, so a Supabase-authority reset fences the user's frozen delivery-record sessions
  only. Copy that metadata before activation if those tombstones matter.
- Neon transcript data of deleted sessions stays in Neon until Neon `sessions` is dropped (precondition 4).
