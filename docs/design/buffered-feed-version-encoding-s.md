# Design: buffered feed version encoding S with fail-closed same-second refusal

For: schema owner. Read-only design, no code. Inputs: /tmp/r-feed-version.md sections 1-9; branch
`fix/buffered-feed-version-order-20261108` at 27b0bcf1 (PR #382, clone the #382 checkout), main at
main. Path abbreviations: `F` = src/session-pipeline/buffered-search-feed.ts (#382),
`M870` = supabase/migrations/20261105043870_buffered_search_feed_cursor.sql, `T` =
src/session-pipeline/__tests__/buffered-feed-version.clickhouse.test.ts (#382).

## 0. What #382 has, and what it does not

- Has: default_kind hardening (`F` `ORIGIN_COLUMNS` l.85, `assertOriginColumns` l.333). Has real-CH defect fixtures
  (`T` CASES matrix; d1/d2/d3; the `sec-*`/`off-*`/`band-*` ENC_CASES; the pure-bounds test).
- Does not have an encoding fix. `projectBufferedStage` still stamps `version = Date.parse(header.received_at)`
  (`F` l.286). `received_at` is the receiver's clock (r-feed-version.md section 2), not PG `created_at`.
- The `F` header comment ("Idempotency", l.36-40) still claims that "any later canonical pass wins". That claim is
  what the defect disproves, so it must be rewritten together with the fix.

## 1. Domain under S

**Canonical writers today, all raw epoch ms from the writer's wall clock:**
- `indexSearchScope`: `version = at.getTime()` (src/session-pipeline/search-index.ts l.265-266).
- vendored `reconcileSession`: `version = now()` (src/session-pipeline/vendor/search-index/reconcile.ts l.427). It
  feeds `toIndexRow(row, version)` at l.501.
- Every canonical row in production is positive raw ms, per the owner's census (r-feed-version.md section 7).
- The column is `version UInt64` with no DEFAULT, under `ReplacingMergeTree(version)`, keyed by
  `ORDER BY (org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id)`
  (vendor/search-index/schema.ts l.62, l.70-72).
- Exception: the proof synthetic insert leaves `version` = 0 (r-feed-version.md section 2, src/proof/aws).

**S encoding (proposed):** `version_b = floor(epoch_seconds(stage.created_at))`.
- `created_at` comes from the PG stage row, as returned by the `stage` RPC (`M870` l.233) and validated per section 4.
- Today ≈ 1,794,096,000 (2026-11-08T00:00Z).
- It is a pure function of an immutable column (`ingest_stage_intents.created_at DEFAULT clock_timestamp()`,
  supabase/migrations/20261104050000_buffered_upload_receipts.sql l.33). No UPDATE of `created_at` exists. So
  replay, backfill and keep-up all compute the same value, and the exact-version deletes stay valid: `deleteBufferedRows`
  (`F` l.353) and the #383 erasure lane.

**Legacy minimum, L:**
- Taken from the pending counts-only production read (authorized; it is run by the credentialed operator only). It
  should return, for `origin='canonical'`: `min(version)`, `count()`, `countIf(version = 0)` and
  `countIf(version < 1e12)`.
- The same read should also return, for `origin='buffered'`: `count()` and `max(version)`.
- **Recommendation: exclude, or separately count, version 0.** The proof synthetic rows have version 0. If any exist,
  `min` = 0, which proves nothing.
- **Activation gate (schema-owner ruling Q1, section 9):** `L_pinned` may be pinned only if the read proves canonical
  `count() > 0`, `countIf(version = 0) = 0`, `countIf(version < 1e12) = 0` and buffered `count() = 0`. Any zero or
  low-domain canonical row, or any existing buffered row, blocks activation. Such rows need a separate, reviewed
  remediation; this design does not authorize one.

**What the buffered band must satisfy:**
- (B1) `0 < version_b < L_pinned`, where `L_pinned` = the exact positive canonical minimum proved by the read, with
  no safety factor, pinned as a source constant (`BUFFERED_VERSION_CEILING`) with the read's date, and only once the
  activation gate above has passed. The encoder refuses outside this range with
  `buffered_version_out_of_band`.
- At L ≥ 1.7357e12 (the `T` stand-in `LEGACY_LOW`), S reaches the ceiling only around the year 57,000 (`T`
  pure-bounds test).
- (B2) No buffered row already in the table sits above the band. The feeder is "not enabled anywhere" (PR #383 body),
  but the read must confirm `count(origin='buffered') = 0`. Otherwise activation is blocked: any existing buffered
  row needs a separate, reviewed remediation before the flag can go on (ruling Q1).
- (B3) Future canonical writers keep writing raw-ms `now()`, never a value below `L_pinned`. RHC does not validate the
  domain today (r-feed-version.md section 7), and a CH CONSTRAINT would be a schema change, which is frozen. A review
  rule alone is insufficient (ruling R1). B3 is enforced by:
  - One shared RHC canonical-version guard, used by backfill and by reconcile/`toIndexRow`, and by the vendored pipeline
    copy (`indexSearchScope` in `search-index.ts`, `reconcileSession` in `vendor/search-index/reconcile.ts`). It refuses
    any version that is not a safe integer, is not positive, or is below `L_pinned`.
  - A census/CI guard that enumerates every direct writer of `transcript_events.version` and fails if any writer
    bypasses the shared guard.
  - This is RHC source work in a separate PR, owned by the relayhistory-cloud-owner lane. It is a prerequisite for
    activation (section 10), not part of the S PR.

## 2. Strict total order source

The owner's requirement (a): strict total order for every divergent stage, or a unique monotonic sequence below L.

**(i) Encode `created_at` at µs: does not fit.**
- 2026-11-08 = 1,794,096,000 s, which is 1.794e15 µs. That is about 1,034 times the stand-in L of 1.7357e12.
- Any real ms-domain L is below about 1.8e12, so µs can never fit.
- With an offset epoch E = 2026-01-01, µs today is 26,870,400 s × 1e6 = 2.687e13, which is already above 1.7357e12.
  The usable headroom at µs is 1.7357e12 µs, about 20.1 days.
- **Rejected.** This agrees with VR (r-feed-version.md section 8).

**(ii) A new PG per-stage monotonic sequence:**
- What it would take: a new migration (next free slot 050000, source-only under the freeze). It would add `feed_seq
  bigint` to `sessions.ingest_stage_intents`, and change `buffered_search_feed_stage` (and `page`) to return it.
- Option (ii-a), assign at insert (DEFAULT nextval):
  - Adding an identity or DEFAULT-nextval column to a populated table rewrites it under ACCESS EXCLUSIVE.
  - All five INSERT sites (20261104050000 l.414, 20261104051000 l.249, 20261104055000 l.873/l.1691, 20261105043850 l.1259) inherit the column.
  - `nextval` order is not commit order and is not `created_at` order under concurrency. That replaces the authority
    the owner accepted (`created_at`) with a different one.
- Option (ii-b), assign lazily inside `stage()`:
  - Turns a read RPC that takes no row lock into a writer (`M870` header l.29-31: "page and stage take no row lock").
  - The order becomes "first feeder observation", which contradicts `created_at` authority.
- Values are about the number of stages, so they fit below L trivially, and they are unique.
- Rollback and replay:
  - Replay is fine while the column persists.
  - A down-migration drops the values. Re-applying it renumbers them, so the exact-version deletes (`deleteBufferedRows`,
    #383) no longer match rows already in CH.
  - A PG point-in-time restore rewinds the sequence below versions already in CH, so new stages lose to older ones.
- **Not recommended** under the freeze.

**(iii) Seconds plus a bounded sub-second component:**
- `version = sec × K + floor(subsec × K)` needs `(sec_max + 1) × K < L`.
- At L = 1.7357e12 with a horizon of 2100-01-01 (4,102,444,800 s): K ≤ 423, so at most 8 bits (K = 256, about 3.9 ms
  buckets).
- With a horizon of 2050: K ≤ 687 (K = 512).
- K = 1000 (ms) gives 1.794e12, which is already above 1.7357e12.
- Still lossy, because `clock_timestamp()` is µs, so ties remain. The `stage_token` tie-break still cannot be encoded.
  It reduces tie probability but does not meet (a), so the refusal in section 3 is needed anyway.
- **Rejected.** It adds K as permanent schema knowledge for no proof gain.

**Recommendation: S (whole seconds), plus a race-safe same-second refusal decided in PG (section 3).**
- This meets (a) and (b) on the set of rows actually written. Any two buffered rows that can share a CH identity and
  are both written have distinct seconds, so version order equals `created_at` order.
- Why identities are bounded: CH identity includes `org_id` and `machine_id = liveMachineId(workspace, origin_id,
  user_id)` (`F` `projectBufferedStage` l.288). So two stages can collide only if they share
  (org_id, workspace_id, origin_id, user_id). That is the PG "sibling" set.
- Race safety, the owner's (b), without a CH single-flight:
  - A stage is feedable only after its intent row is committed and acknowledged (`M870` page l.162, stage l.215-216).
  - Take sibling stages A and B. A's check runs after A's intent commit.
  - If A's check precedes B's intent commit, then B's check (after B's acknowledgement) sees A's committed row.
    Otherwise A's check sees B.
  - So of any two conflicting stages, at least one sees the other and is refused before any CH write. No two tied
    writers can both be written.
- The ClickHouse pre-read is not part of the proof (the owner's BLOCK, r-feed-version.md section 8).
- Cost: a migration (section 6), but no new column, no sequence, no backfill, and a trivial rollback.

## 3. Fail-closed same-second handling

**Detection (authoritative):**
- 050000 replaces `sessions.buffered_search_feed_stage` (body at `M870` l.173-234) and adds one output key:
  `sameSecond: {siblings: int, pending: int}`.
- `siblings` counts other `ingest_stage_intents` rows with equal (org_id, workspace_id, origin_id, user_id) and
  `created_at` in `[date_trunc('second', s.created_at), +1s)`.
- It counts every `cleanup_state` (retired or redacted siblings may still have CH rows until cleanup). It excludes only
  `preparing` rows whose `expires_at` has passed, which are the RPC's own `gone`/`stage_expired` (`M870` l.214-215).
- `pending` = siblings still `preparing` and unexpired.
- It is a read only. The existing `(cleanup_state, created_at, stage_token)` index (20261104050000_buffered_upload_receipts.sql l.38) bounds the scan to
  one second per state value.

**Detection (feeder):**
- `sqlBufferedFeedSource.stage` (`F` l.203-258) parses `sameSecond`, required when the new flag is on.
- If the key is absent while the flag is on, it refuses `sql_same_second_unavailable` (fail closed against an
  unapplied migration).
- Equal encoded version inside one page is also checked (section 4), but the PG count is the decision.

**Refuse:**
- Granularity is the whole stage. Identity overlap is not computable in PG without reading S3, and over-refusal is
  safe.
- `pending > 0`: the outcome is "not yet decidable".
  - Keep-up returns `requeue` (`F` `feedBufferedStageByKey` l.462-475 → consumer.ts l.86-90).
  - Backfill sets `summary.blocked = "same_second_pending"` and stops before the stage, using the existing `blocked`
    break at `F` l.524.
  - This is bounded by the sibling's `expires_at` (`clock_timestamp()+120s`, 20261105043850 l.1262).
  - Ruling Q8: the 120 s bound is accepted only with tests proving that a retry after expiry reaches a deterministic
    terminal classification (written, or `same_second_tie` refused). Retry counts and DLQ arrivals for the keep-up
    queue are documented as observable signals (section 3 Observability).
- `siblings > 0 && pending = 0`: refused, deterministically.
  - Intents are never deleted (`ON DELETE RESTRICT`, PR #383 body), so a retry cannot change the answer.
  - No CH insert for this stage. Other stages on the page proceed: they go into `loaded`, while the refused one does not.
  - Reason `same_second_tie` is added via `count()` (`F` l.503).

**Cursor:**
- Never passes the refused stage silently.
- `runBufferedFeed` (`F` l.496-550) may advance past a terminal refusal only after it has durably recorded it in a
  refusal ledger and the record has been read back. The ledger is a row in `search_ingest_state` (vendored schema.ts
  l.27/l.89, the store #383 already uses), keyed `buffered-tie:<stageToken>`, with value
  `{siblings, firstSeen, revisits, state}` and no content.
- If the ledger write fails, the code throws and the cursor stays (`F` l.539 runs after `flush`).
- Alternative: hold the cursor until a human acts. It is rejected because one tie would stall all buffered indexing.
  Ruling Q3 (section 9): accepted. The cursor advances only after the durable ledger write and readback. The refusal
  stays outstanding, the run stays `ok: false`, and it is not coverage (R3).
- Keep-up never records the ledger; it logs and returns `skipped`. The backfill or tail cursor is the single authority
  that classifies the stage, so a keep-up skip never counts as coverage.

**Observability:**
- Existing hooks are structured logs only. Per stage: `log.info("buffered stage search feed", {outcome, reason})` at
  consumer.ts l.92-93. Per run: `log.info("buffered search feed run", {reasons, state, ok})` at lambda.ts l.668-670.
- Add:
  - `log.warn` with `reason: "same_second_tie"` and the counts, with no tokens in content fields.
  - The run state `caught_up_with_refusals`, with `ok: false` while any refusal was recorded this run or is still
    outstanding in the ledger (R3).
  - An EMF metric following the only existing pattern (`96Bulls/BufferedGc`, src/session-ingest/buffered-gc-lambda.ts
    l.11): `96Bulls/BufferedSearchFeed` `SameSecondRefused` (count) and `TieRefusedOutstanding` (gauge). The gauge is
    computed from the durable ledger, not from per-run state.
  - Keep-up retry/DLQ observability (ruling Q8): requeue attempts for `same_second_pending` are logged with the stage
    age, and arrivals on the keep-up DLQ are counted, so a stage that never reaches a terminal classification is visible.
- The alarm is a cloud resource, so it needs Khaliq's OK. Ruling Q9/R4: the durable outstanding gauge and the alarm
  must both exist before enable.

**What the system shows for a refused stage:**
- **Prior visible rows:** unchanged. Canonical rows always outrank by B1. Earlier-second buffered rows of the same
  identities stay visible. If a sibling was written first (it was alone at its check), its rows stay visible exactly
  as written (ruling Q4). The ledger records "sibling visible, this stage refused", naming the newer sibling as the
  refused one.
- **Newer retained evidence:** not indexed. The ledger, the metric and `ok: false` say so explicitly, and the stage is
  never reported as "indexed" or "covered".
- **Replay:** re-running keep-up or backfill on the refused stage gives the same PG answer, so nothing is written.
  Replaying the visible sibling re-inserts byte-identical rows (same `created_at`, so the same version; with #383 the
  same stageToken fingerprint), which fold.
- **Cleanup:**
  - #383 erasure deletes only rows that exist. The refused stage has none, so there is nothing to do. The sibling is
    handled normally.
  - Buffer GC replacement inserts a new intent with a fresh `created_at` (20261104051000_ingest_buffer_gc.sql l.249-252). It usually lands in a new
    second and is indexed normally, which resolves the tie.
  - Publication (`batch_not_buffered`) hands the records to canonical, which outranks them.

**Recovery, bounded:**
- At most R ledger entries per run (proposed R = 20) are revisited by calling `stage({stageToken})`:
  - `gone` → `resolved_gone` (canonical or erasure owns them).
  - `deferred` with `gc_in_flight`, or a replacement token present → `resolved_replaced`.
  - Still `live` with siblings → `revisits += 1`.
- After N revisits (proposed N = 24, hourly cadence) the state becomes `refused_terminal`. It stays in the outstanding
  gauge until publication.
- A refused stage is never auto-indexed. No path reports eventual success.
- Mutual refusal (both siblings refused, section 2) is accepted by the schema owner as the fail-closed availability
  trade-off (ruling Q9/R4), conditional on `ok: false`, the durable outstanding gauge and the alarm existing before
  enable. It is never called covered.

## 4. Feeder hardening (c)/(d)

**(c) Raw physical canonical check before a buffered write:**
- New `F` function `canonicalPresent(ch, rows)`, called at the top of `writeAndRecheck` (`F` l.442) before `ch.insert`.
- It runs `SELECT <identity tuple> FROM transcript_events WHERE <identity> IN [...] AND origin <> 'buffered'`, with no
  FINAL. That reads every physical part, including unmerged ones. Lightweight-deleted rows are masked.
- It uses the same tuple shape as `deleteBufferedRows` (`F` l.360-369).
- Matched rows are dropped from the insert and counted `canonical_present`. Only those identities count as covered,
  because the canonical copy is the publication.
- This is defense in depth. Safety against a racing canonical write comes from B1, not from this check.

**(d1) Exact createdAt grammar, on both the page and keep-up paths:**
- `ISO` (`F` l.116) accepts 1 to 6 fractional digits and no fraction at all, and page order compares strings (`F`
  l.223-224).
- With mixed lengths, a lexical compare misorders: `"…:00Z" > "…:00.1Z"` because 'Z' sorts after '.'.
- The RPCs always emit `.US` (6 digits) (`M870` l.157, l.233). So add
  `CREATED_AT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/`, plus a calendar round-trip (reformat the parsed
  seconds and compare the first 19 characters), in `page` (l.218), in `stage` (`!str(s.createdAt)` at l.236 today, so
  keep-up is effectively unvalidated), and for `after` (l.225).
- The encoder must not use `Date.parse` for the version. Parse the fixed fields, `Date.UTC(...) / 1000`, and refuse
  outside B1.

**(d2) Bind the page candidate to the stage RPC result:**
- In `runBufferedFeed` after `stage({stageToken})` (`F` l.519), require that `state.stage.stageToken`, `createdAt`,
  `tenant`, `originId` and `batchId` equal the candidate's.
- On a mismatch, throw `sql_stage_page_mismatch` (not skip). The cursor is not advanced.
- Today only the token is bound (`sql_stage_identity`, l.240-241).

**(d3) StageToken tie in page-order validation:**
- Replace `F` l.223-224 with a strict keyset check: for each i, `(createdAt[i-1], token[i-1]) < (createdAt[i], token[i])`,
  and the first row must be above `after`. Equal pairs and descending tokens at equal `createdAt` give `sql_page_order`.
- PG uuid order is byte order, which equals lowercase-hex lexical order, so string compare is valid after UUID
  validation.
- Also flag equal encoded seconds between consecutive candidates that share the sibling tuple, as a cross-check on the
  PG count. If PG says 0 there, refuse with `same_second_count_disagrees`.

**Version source:**
- `projectBufferedStage(decoded, retained, keyRoot)` (`F` l.283) gains a `version` argument, computed by the new
  `encodeBufferedVersion(state.stage.createdAt)`.
- `header.received_at` stays parse-checked but is no longer the version.
- `loadLive` (l.418) passes it.

## 5. Test plan (e)

**Causal shape:**
- Every new CH test runs one body under two encoders: legacy (`Date.parse(received_at)`, flag off) and S (flag on).
- Legacy asserts the defect (canonical gone, or older stage survives). S asserts the fix.
- The tie test also runs with the sibling count forced to 0 (an injected source) and asserts that the older stage wins
  after OPTIMIZE. That proves the refusal is what flips it.
- PR sequence: a test-only commit with the S assertions marked expected-fail, then a CI run ID showing red, then the
  fix commit with a green run ID. Both IDs go in the PR body.

**Already on #382 (real CH, CI only):**
- Version matrix a…skew-behind, with raw/FINAL/read API before and after OPTIMIZE FINAL.
- d1/d2/d3 through the real `feedBufferedStageByKey`, `indexSearchScope` and `deleteBufferedRows`.
- `sec-*`/`off-*`/`band-*` encoding cases.
- The pure-bounds test (runs without CH).
- These stay as legacy-encoder documentation. The fix flips d2/d3 and `sec-tie-*` only under the flag.

**New, real CH (needs `READ_API_TEST_CLICKHOUSE_URL`, CI jobs at .github/workflows/ci.yml l.46/l.114):**
1. Canonical before and after: S buffered vs canonical at `L_pinned`-adjacent raw ms, both insert orders, raw, FINAL,
   read API, OPTIMIZE FINAL.
2. Same-second siblings A and B with identical records, fed concurrently (`Promise.all` of keep-up A and backfill B over
   a live pglite or PG source): at most one physical buffered row per identity; refusal ledger row present; cursor not
   past B until the ledger write; legacy encoder shows last-insert-wins.
3. Replay of the refused stage, then OPTIMIZE: zero rows from it. Replay of the visible sibling folds byte-identically.
4. Guard (c): a canonical row in an unmerged part (STOP MERGES) means the buffered write is refused for those
   identities.
5. Reconcile `deleteRows`, then a still-live replay (blocker 3): under S, after a later canonical re-insert, canonical
   wins in both orders. The resurrection itself is asserted as owned by the privacy fence or #383, not by the encoding.
6. GC replacement in a new second resolves the tie (`resolved_replaced`).
7. Canonical version=0/low-domain activation refusal (ruling Q1): with a canonical row at version 0, or at a
   low-domain value below 1e12, or with any existing buffered row, the activation check refuses to arm the feeder
   and no buffered row is written. Causal: the same fixture without the offending row arms and writes.
8. Late-ack revisit (ruling Q7/R2): a stage registered before the cursor and acknowledged after it passed is
   revisited by the durable late-ack strategy and classified (written or refused), never left behind the cursor.
   Causal: with the revisit disabled the stage stays unclassified. Lands with the late-ack PR.

**New, RHC shared writer guard (separate RHC PR, ruling R1):**
- The shared canonical-version guard refuses a non-safe integer, 0, a negative value and a value below `L_pinned`, and
  accepts raw-ms `now()`. Each refusal is asserted through backfill, reconcile/`toIndexRow` and the vendored pipeline
  copy.
- Causal: the census/CI guard fails on a fixture writer that bypasses the shared guard and passes once it is routed
  through it.

**New, pglite (src/session-pipeline/__tests__/buffered-search-feed.pglite.test.ts, which already inserts stages with an
explicit `created_at`, l.49):**
- 050000 `sameSecond` counts: same tuple and same second → 1. Different user or origin → 0. Expired preparing → 0.
  Unexpired preparing → pending 1. Retired → counted. GC parent or replacement of the same lineage in the same second
  → counted (ruling Q2).
- Grants unchanged.

**New, unit (`buffered-search-feed.test.ts`, fake source):**
- The grammar (1-5 digits, no fraction, Feb 30 all refused).
- Page keyset ties and duplicates; candidate/stage binding mismatch.
- Encoder band edges (0, `L_pinned`).
- Pending → requeue/blocked. Terminal → ledger, then advance. Ledger failure → no advance. Ledger readback mismatch →
  no advance.
- Retry after the sibling's 120 s expiry reaches a deterministic terminal classification (ruling Q8).
- A run with an outstanding refusal reports `ok: false` even when it recorded no new refusal (R3).
- Bounded revisit → `refused_terminal`.
- Log and metric fields contain no content.

## 6. Rollout

- **Flag:** `INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION`, literal `off` (default) or `on`, parsed like `F` l.135-137.
  - `INGEST_BUFFERED_SEARCH_FEED=on` is refused (`version_encoding_not_armed`) unless this flag is `on`, so the
    defective legacy encoding can never run armed.
  - Add it to the lambda.ts l.651-660 pre-secret guard.
  - The guard (c) and the refusal ledger ride on the same flag. Recommendation: no separate flags, because each is
    meaningless without S (Q5).
- **Migration:** yes, slot 050000 (registry: next free).
  - It does CREATE OR REPLACE of `buffered_search_feed_stage` only: no table change, no grant change.
  - Rollback file `supabase/rollbacks/20261108050000_*.down.sql` restores the 043870 body.
  - It changes the migration-set generation (`M870` l.33-34), so check the src/hoopdb/migration-sentinels.ts and
    function-boundary sync.
  - Source-only under the freeze. Applying it needs Khaliq's explicit OK.
- **#382:** merge first, as the diagnosis plus default_kind hardening PR (VR: "not an encoding fix").
  - The S PR is separate and flips the DEFECT assertions only under the flag.
  - Folding it in would mix a source-only migration into a reviewed diagnosis PR.
- **#383:** land after the S PR, then rebase it.
  - Its exact-version deletes and its `bf<body>:<stageToken>:<ordinals>` fingerprint (PR body) must be re-run against
    S versions.
  - Its stageToken fingerprint makes tied sibling rows non-byte-identical, which is one more reason ties must never be
    written.
- **Stays frozen:**
  - CH schema: no ORDER BY, column, or constraint change.
  - Canonical writers (raw ms, B3). The shared RHC guard is separate RHC source work (section 1, B3).
  - Applying 050000 and any B2 or low-domain canonical remediation.
  - Enabling the feeder or the erasure lane in any environment.
  - The feeder login bootstrap.
  - Any CloudWatch alarm.
  - Path R / G4 / G2 (r-feed-version.md section 9).

## 7. Open questions (each with a recommended answer)

- **Q1. L source.** Is `min(version) WHERE origin='canonical' AND version > 0` (with version 0 counted separately)
  acceptable as the proof, pinned with a safety factor?
  - Recommend: yes. Pin `L_pinned = proved min`, with no factor needed, because S sits about 1,000 times lower.
  - Ruling: accepted with condition (gated on the read in section 1; section 9).
- **Q2. Sibling scope.** Should the stage's own batch lineage (GC parent or replacement, same `batch_id`) count as
  siblings?
  - Recommend: count them (fail closed). A same-second GC replacement is implausible, and over-refusal is visible.
  - Ruling: accepted (section 9).
- **Q3. Cursor policy on terminal refusal.** Advance with a durable ledger record and the metric, or hold?
  - Recommend: advance after the ledger write is read back. Holding stalls everything behind one tie.
  - Ruling: accepted with condition (refusal stays `ok: false` and is not coverage; section 9).
- **Q4. Visible first-writer sibling.** When A was written alone and B is later refused, keep A or delete A's rows
  exactly?
  - Recommend: keep. A is committed retained evidence, the CH table holds no tie, and the ledger records it.
  - Ruling: accepted (keep A exactly; ledger states the newer sibling was refused; section 9).
- **Q5. One flag or three** (encoding, guard (c), ledger)?
  - Recommend: one, because the guard and ledger have no meaning without S, and "each new behaviour gets its own flag"
    is met by a single new flag that is off by default.
  - Ruling: accepted (section 9).
- **Q6. Refusal granularity.** Whole stage, or only overlapping identities (by reading the sibling's S3 object)?
  - Recommend: whole stage now. Measure frequency with a counts-only read of same-second sibling pairs since cutover
    before refining.
  - Ruling: accepted with condition (the counts-only tie-frequency measurement is required before enable; section 9).
- **Q7. Pre-existing gap.** `page` lists only acknowledged stages (`M870` l.162). A stage registered at t1 and
  acknowledged after the cursor passed t1 is never revisited by the backfill. Keep-up covers it, but a keep-up skip
  does not count as coverage.
  - Recommend (superseded): track it as a separate item. Bound it by holding the cursor at `now() − 120 s − margin`,
    past the preparing expiry. It is independent of the encoding but relevant to VR's "never classify unindexed as
    covered".
  - Ruling: rejected as closure. A fixed margin does not bound late acknowledgment (R2). A durable late-ack revisit or
    watermark strategy is required. It may land in a separate PR, but feeder activation stays blocked until it lands
    and proves that an acknowledged stage cannot fall permanently behind the cursor. A keep-up skip is not coverage.
- **Q8. Keep-up requeue is unbounded** (lambda.ts l.348-354: DelaySeconds with no attempt count).
  - Recommend: rely on the 120 s preparing expiry bound for `same_second_pending`, and document it. No attempt counter
    is needed for this design.
  - Ruling: accepted with condition (tests prove a retry after expiry reaches a deterministic terminal classification;
    retry/DLQ observability documented in section 3; section 9).
- **Q9. Mutual refusal.** Both same-second siblings can be refused (section 2, R4). Accept this as the fail-closed
  availability trade-off?
  - Recommend: accept, with `ok: false`, the durable outstanding gauge and an alarm; never report it as covered.
  - Ruling: accepted with condition (`ok: false`, durable outstanding gauge and alarm exist before enable; never called
    covered, never auto-indexed; section 9).

## 8. Residuals recorded by the verify peer (codex-relayhistory-verify-replica, relay message 245248035358314496)

These were open when recorded. Each item's status below follows the schema-owner ruling (section 9). None is
presented as closed by implementation; conditions marked as prerequisites remain unmet until section 10 records them.

- **R1. L is evidence, not an invariant.** Pinning the observed positive canonical minimum describes current rows. It
  is not a lower bound for every future canonical writer, import, reconcile pass or clock. The S PR must document the
  complete canonical writer rule (every writer stamps raw epoch milliseconds from a sane clock, never a value below
  `L_pinned`) and the consequence of a future below-L canonical value: that row loses to any buffered row of the same
  identity with a higher S value, so the publication is no longer guaranteed to outrank. A review rule on
  `search-index.ts` and `reconcile.ts` is the only enforcement while the ClickHouse schema is frozen.
  - Status: accepted with condition. The review rule alone is ruled insufficient. Replaced by the shared RHC
    canonical-version guard plus a census/CI guard (section 1, B3), RHC source work in a separate PR owned by the
    relayhistory-cloud-owner lane, and a prerequisite for activation (section 10).
- **R2. A fixed margin does not bound late acknowledgment.** Holding the cursor at `now() - 120 s - margin` does not
  cover a stage whose `created_at` is older than the cursor and whose acknowledgment arrives arbitrarily late, unless a
  maximum acknowledgment delay is proved. A keep-up skip is not coverage. The S PR must either ship a durable revisit or
  watermark strategy for late acknowledgments, or state the source liveness gap explicitly. It must not claim
  completion by margin.
  - Status: fixed margin rejected as closure. A durable late-ack revisit or watermark strategy is required (may be a
    separate PR); feeder activation is blocked until it lands with proof (Q7, section 10).
- **R3. A refusal is not coverage.** A run that recorded or still carries an unresolved refusal stays `ok: false`, and
  the refusal stays outstanding, until the stage is actually resolved (publication, erasure, replacement). Advancing the
  cursor after the ledger write does not close the refusal.
  - Status: accepted. The cursor advances only after durable ledger write and readback; the refusal stays `ok: false`
    and is not coverage (Q3, section 3).
- **R4. Mutual refusal availability.** Both same-second siblings can be refused (section 2). This is fail-closed, but
  the availability trade-off needs the schema owner's explicit agreement before code.
  - Status: accepted with condition. Explicitly accepted as the fail-closed availability trade-off, conditional on
    `ok: false`, a durable outstanding gauge and an alarm existing before enable; never called covered, never
    auto-indexed (Q9, section 9).
- No ClickHouse, SQL, apply or flag action follows from this document. #382 stays a diagnosis PR. No Path R dependency.

## 9. Schema-owner ruling (2026-11-08) and incorporated conditions

Source: schema-owner ruling on #385, relay message 245252322769489920 (relayhistory-cloud-owner, 2026-11-08).
Overall: conditional accept of S plus the PG same-second refusal. The implementation and review-ready claim stays
blocked until the conditions below are incorporated. #382 remains diagnostic only.

| Item | Ruling (one line) | Status | Reflected in |
| --- | --- | --- | --- |
| Q1 | Pin the exact positive canonical minimum, no factor, only if the read proves canonical count>0, countIf(version=0)=0, countIf(version<1e12)=0 and buffered count=0; any zero/low canonical or existing buffered row blocks activation and needs separate reviewed remediation. | accepted with condition | 1 (L, B1, B2), 5 (test 7), 7, 10 |
| Q2 | Include GC parent/replacement lineage as siblings. | accepted | 5 (pglite), 7 |
| Q3 | Advance only after durable ledger write and readback; the outstanding refusal remains ok:false and is not coverage (R3). | accepted with condition | 3 (Cursor, Observability), 5, 7, 8 |
| Q4 | Keep the first-written sibling A exactly; the ledger states the newer sibling was refused. | accepted | 3 (What the system shows), 7 |
| Q5 | One default-off flag, `INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION`. | accepted | 6, 7 |
| Q6 | Whole-stage refusal; the counts-only tie-frequency measurement is required before enable. | accepted with condition | 7, 10 |
| Q7 | Fixed margin rejected as closure; a durable late-ack revisit/watermark strategy is required (may be a separate PR) and feeder activation stays blocked until it lands and proves an acknowledged stage cannot fall permanently behind the cursor. | rejected (replaced by a required strategy) | 5 (test 8), 7, 8 (R2), 10 |
| Q8 | 120 s pending expiry accepted only if tests prove a retry after expiry reaches a deterministic terminal classification; document retry/DLQ observability. | accepted with condition | 3 (Refuse, Observability), 5 (unit), 7, 10 |
| Q9 | Mutual refusal explicitly accepted as the fail-closed availability trade-off, conditional on ok:false, a durable outstanding gauge and an alarm existing before enable; never called covered or auto-indexed. | accepted with condition | 3 (Observability, Recovery), 7, 8 (R4), 10 |
| R1 | Review-rule-only B3 is insufficient; add one shared RHC canonical-version guard (backfill, reconcile/toIndexRow, vendored pipeline copy) refusing non-safe/nonpositive or below-L_pinned values, plus a census/CI guard for every direct writer; RHC source work, separate PR. | accepted with condition (review rule rejected) | 1 (B3), 5 (RHC guard tests), 6, 8, 10 |
| R2 | Same as Q7: durable late-ack revisit/watermark required; no completion by margin. | rejected as closure (replaced by a required strategy) | 7 (Q7), 8, 10 |
| R3 | A refusal is not coverage; ok:false while outstanding. | accepted | 3, 5, 8 |
| R4 | Same as Q9: mutual refusal accepted as the fail-closed trade-off under the stated conditions. | accepted with condition | 3, 7 (Q9), 8, 10 |

Accepted unchanged: feeder hardening (c)/(d) (section 4), the exact createdAt grammar (d1), binding the page candidate
to the stage RPC result (d2), the strict keyset (d3), the B1 encoder refusal (section 1), one-flag arming (section 6)
and the test plan (section 5). The ruling adds two causal tests, both now in section 5: canonical version=0/low-domain
activation refusal (test 7) and the shared writer guard (RHC guard tests).

## 10. Activation prerequisites

In order. Each item must be recorded as met, with its evidence, before the next one is relied on.

- [x] 1. Operator counts-only read result: canonical count>0, countIf(version=0)=0, countIf(version<1e12)=0, buffered
  count=0. Otherwise stop: separate reviewed remediation first (Q1).
  - Pin record: counts-only ClickHouse read 2026-11-10 16:20Z by the qualified production actor
    (brian-db-takeover-linuyc): canonical count 35658395, canonical min(version) 1793454538262,
    countIf(version = 0) 0, countIf(version < 1e12) 0, buffered count 0 (max 0), origin columns PASS; read
    window 2026-11-10T16:20:08.718Z to 16:20:12.614Z. Receipts: sanitized census receipt
    clickhouse-ceiling-census-transfer.json sha256
    d8418d37e2c90f0a69d88fd828986adf5569683e921543a138fe8bdb5ab66dad; raw actor receipt
    root-clickhouse-feeder-ceiling-20261110T162012Z.json sha256
    d1a171bd74da467f1459604525dca3e377b69b43db81fe03338f08071cc16444 (held by the actor, not copied).
    Reported over Agent Relay by the actor (messages 245949774188331008 and 245950211726024704). The gate passes;
    `BUFFERED_VERSION_CEILING` = 1793454538262 (exact minimum, no safety factor, B1) is pinned in this PR.
    The pin arms nothing: both feed flags stay default off.
- [ ] 2. RHC shared canonical-version guard PR merged, with the census/CI guard over every direct writer (R1;
  relayhistory-cloud-owner lane).
- [ ] 3. Durable late-ack revisit/watermark strategy landed, with proof that an acknowledged stage cannot fall
  permanently behind the cursor (Q7/R2).
- [ ] 4. Counts-only tie-frequency measurement of same-second sibling pairs recorded (Q6).
- [ ] 5. Durable `TieRefusedOutstanding` gauge and its alarm exist (Q9/R4; the alarm is a cloud resource and needs
  Khaliq's OK).
- [ ] 6. Expiry-to-terminal tests green: a retry after the 120 s pending expiry reaches a deterministic terminal
  classification (Q8).
- [ ] 7. Flag flip of `INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION`: only on Khaliq's word. It is not this lane's
  authority.

No code, apply or flag authority follows from the ruling or from this document. The revised docs head goes to the
schema owner before any semantic code is started.
