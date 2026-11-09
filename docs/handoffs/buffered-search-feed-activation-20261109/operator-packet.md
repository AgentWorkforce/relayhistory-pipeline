# Buffered search feed: production activation packet

Scope: making post-cutover sessions findable in ClickHouse (CH) search. The fast fix is to
activate the CH feed that already exists, adding as little new CH work as possible. Today any
session accepted through the buffered path since the cutover (2026-11-05 05:33:45Z,
`src/session-pipeline/buffered-search-feed.ts:81`) is missing from CH search. Buffered stages
are never published to manifests, and the canonical indexer only fires on `/manifest.json`
(`stacks/aws/session-pipeline.ts:310-326`). Khaliq: "search is extremely important too, kick
that off concurrently". This packet covers the order, bounds, stop rules, canaries, rollbacks
and the code gaps. Citations are `path:line` against main 320f33c6. Ranges are approximate.

## 0. Summary and decision table

* Code only. Nothing in this packet has been applied, deployed or enabled anywhere in
  production.
* Every apply, flag flip, enable and deploy needs Khaliq's own explicit OK, in his own words,
  given in Brian's session. A relayed or paraphrased OK does not count, and neither does an OK
  given for an earlier step.
* Brian is the only DB operator and the only reader deployer. No other agent or person connects
  to the production database or ClickHouse for any step here.
* **Schema-window freeze (standing).** No production apply of 043850 or later until the 044030
  overlap is applied. 043860, 043870 and 043890 are all later than 043850, so they ride
  window 1: the contiguous ledgered apply 043850..044030, under the one accepted outage
  (`docs/design/zero-503-schema-windows.md:205-240`).
* `SEARCH_INDEX_ENABLED=1` is already live in prod. Every new behaviour below gets its own
  default-off flag. Landed migration bytes are frozen.
* Report counts, durations and pass/fail only. Never select, log or paste claim tokens, stage
  tokens, cursor positions, tenant ids, session ids, record contents, generation hashes, hosts,
  endpoints or project refs.

**Verdict: the feeder must NOT be enabled yet.** Steps 1-4 below (CH columns, window 1, reader
gate, role) are safe to do, and useful as prerequisites. Step 5 (enable
`INGEST_BUFFERED_SEARCH_FEED`) is blocked by three code gaps (section 7: G1 version order,
G2 late-ack skip, G3 erasure lane) and one unverified reader assumption (G4). Today, enabling it
would write buffered rows that a later canonical pass cannot win over (#382's tests prove
this). It would also skip late-acknowledged stages for good, and leave erased-session rows
physically in CH.

"Carries over" means the step's cost is kept if the search backend changes later (for example
to OpenSearch). "WASTED" means the cost is specific to CH and is lost on a backend change. CH
stays the incumbent this week; no replacement is planned.

| # | Step | Gate | Blocked by | Backend change | Section |
|---|------|------|------------|----------------|---------|
| 1 | CH `ALTER TABLE ADD COLUMN IF NOT EXISTS origin, origin_claim_token` | Khaliq | nothing | WASTED (small, acceptable, CH remains incumbent) | 1 |
| 2 | Pre-check: the feed role exists (counts-only) | none (read) | nothing | carries over | 2.3 |
| 3 | Window 1 (043850..044030, includes 043860/043870/043890) | Khaliq | the window 1 plan | carries over | 2 |
| 4 | Reader deploy with `READ_API_BUFFERED_SEARCH_GATE=on` | Khaliq | steps 1 and 3 | mostly carries over (gate SQL and rules); the origin columns in the CH query are WASTED | 3 |
| 5 | Feed role `LOGIN` plus the secret fields | Khaliq | step 3 | carries over | 2.4 |
| 6 | Feeder rollout (DISABLED rule) | Khaliq | none in code | carries over (the scheduler); the CH sink is WASTED | 4 |
| 7 | Feeder enable (`INGEST_BUFFERED_SEARCH_FEED=on`) | Khaliq | **G1, G2, G3, G4** | the projection and cursor carry over; the CH insert is WASTED | 4, 5 |
| 8 | Backfill from 2026-11-05 05:33:45Z | Khaliq (the same enable) | step 7 | the cursor and projection carry over; the CH inserts are WASTED | 5 |

PR verdicts (ADDENDUM A). Detail is in section 7.

* **#383** (feat/buffered-erasure-delete-20261108, head 725f10a8): **MUST land before the
  feeder enable.** The runbook forbids arming without a CH later-erasure delete lane
  (`runbooks/sessions-prod-session-pipeline.md:268-269`). #383 also changes the buffered
  fingerprint to `bf<body>:<stageToken>:<ords>`. Rows written before it count as
  `legacy_unmapped` and #383 never deletes them, so landing it after the enable leaves
  permanently undeletable rows.
* **#382** (fix/buffered-feed-version-order-20261108, head 27b0bcf1): **advisable, not
  blocking on its own.** It contains no fix. It adds a `default_kind = 'DEFAULT'` check to
  `assertOriginColumns` and a real-CH DEFECT/SAFE/RECOVERY test matrix that proves the
  version-order defect. That matrix is the regression base for the G1 fix, so the G1 fix
  should land on top of it. CH-only test: small, acceptable, CH remains incumbent.
* **#385** (docs/buffered-feed-version-s-design-20261108, head c647a984): **not superseded;
  advisable to land as the design record.** The implementation it specifies (S encoding plus
  its section 10 prerequisites) is a MUST before the enable. The S encoding itself is
  ReplacingMergeTree-specific (WASTED on a backend change); its PG same-second refusal carries
  over.

## 1. ClickHouse DDL

**Statement (the only CH DDL in this packet).** From
`docs/proposals/buffered-search-feed.clickhouse.draft.sql:7-8`:

```sql
ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin LowCardinality(String) DEFAULT 'canonical';
ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin_claim_token String DEFAULT '';
```

* **What it is.** A draft, applied by an operator with Khaliq's OK *before* the feed flag. It is
  additive and idempotent. `schema.ts` is not edited, and the feeder checks `system.columns`
  instead (`docs/proposals/buffered-search-feed.clickhouse.draft.sql:1-6`). The same DDL runs
  in the real-CH test (`src/session-read-api/__tests__/buffered-origin.clickhouse.test.ts:19-22,47`)
  against CI's ClickHouse 26.8 image (`.github/workflows/ci.yml:58,136`).
* **No CH migration mechanism in this repo.** `migrateClickHouse`
  (`src/session-pipeline/reconcile.ts:53`) has no caller here, and it only issues
  `CREATE TABLE IF NOT EXISTS`. So this is a manual statement, run by Brian with Khaliq's OK.
  There is no ledger row.
* **Cost.** On MergeTree, `ADD COLUMN` with a constant DEFAULT is a metadata-only change; old
  parts read the default. No mutation and no rewrite are expected. That is standard CH
  behaviour, not something measured on the production cluster. Origin columns are not in the
  sort key, so the ORDER BY does not change (the table is ReplacingMergeTree(version),
  `PARTITION BY toYYYYMM(ts)`).
* **Effect on the live reader (gate off).** Canonical mode adds `origin = 'canonical'`. Every
  existing row reads the default `'canonical'`, so results do not change. Before the DDL, the
  canonical-mode query gets a missing-column error (codes 8/16/47,
  `src/session-read-api/queries.ts:335-337`) and falls back to legacy plus a probe
  (`queries.ts:339-343`). If the column appears between the error and the probe, the request
  throws `origin_column_changed` once. So expect a brief blip at the DDL moment. Its rate is
  unmeasured; it is one request per race.
* **Must precede the gate.** Gate mode names the origin columns and has **no legacy fallback**
  (`src/session-read-api/page.ts:160-185`, `queries.ts:312-333`). Gate on without the columns
  means every gated search returns 503.
* **Feeder refusal without it.** `assertOriginColumns` (`src/session-pipeline/buffered-search-feed.ts:324-333`)
  refuses the run if the columns are absent or have the wrong type. With #382 it also refuses a
  MATERIALIZED or ALIAS column.

**Canary (counts only):**

```sql
SELECT name, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 'transcript_events'
  AND name IN ('origin', 'origin_claim_token');
-- expect 2 rows: LowCardinality(String)/DEFAULT/'canonical' and String/DEFAULT/''
SELECT origin, count() FROM transcript_events WHERE ts >= now() - INTERVAL 1 DAY GROUP BY origin;
-- expect only 'canonical' before the feeder runs
```

**Rollback.** None needed while no buffered rows exist. `DROP COLUMN` would break the gate (503)
and the feeder. If it is ever needed: gate off first (section 3), then feeder off, then drop.
Each step needs Khaliq.

**Benchmark framing.** This DDL is the minimum new CH work. It is CH-only, small and
acceptable; CH remains the incumbent.

## 2. Postgres side: 043860 / 043870 / 043890

### 2.1 What the three slots are

| Slot | Object | Sentinel (`src/hoopdb/migration-sentinels.ts`) | Purpose |
|------|--------|-----------------------------------------------|---------|
| 043860 | `sessions.buffered_search_gate_for(...)` (11 args) | `:207` `to_regprocedure(...) IS NOT NULL` | gate verdicts for the reader (`src/session-read-api/buffered-search-gate.ts:245-254`) |
| 043870 | `sessions.buffered_search_feed_cursors` plus 3 RPCs (page, stage, advance) | `:208` relation | feed cursor and stage listing (`supabase/migrations/20261105043870_buffered_search_feed_cursor.sql:1-58`) |
| 043890 | 043860's function, redefined with the retired-claim witness | `:214-217` `prosecdef` plus `prosrc LIKE '%ingest_bookkeeping_retired_claims%'` | gate rule for retired claims |

* All three are later than 043850, so they wait for window 1. They cannot go out of band. The
  migrator's `sequencePreflight` runs every unledgered step's sentinel and refuses the whole run
  with `unledgered_schema` on any hit (`src/hoopdb/migrate-guards.ts:1332-1340,1458`). This is
  the same reason as the 044080 packet's section 1.
* `migrate.ts` has no target-slot option, so window 1 applies every pending slot through 044030
  in one owner-apply (`docs/design/zero-503-schema-windows.md:205-240`).

### 2.2 Window 1

Order, per `zero-503-schema-windows.md:205-240`: Worker deploy (G_cur/G5), then an owner-apply of
all pending slots through 044030, then the rebind, then a later Worker deploy. Window 1 does not
depend on the CH DDL, and the CH DDL does not depend on window 1. Either can go first.

Effects specific to these three slots:

* 043860 and 043890 create or replace a function. Nothing calls it until the reader gate is on
  (section 3).
* 043870 creates an empty table and three functions. Nothing calls them until the feeder is
  enabled. It takes `SET LOCAL lock_timeout='5s'` (`20261105043870_buffered_search_feed_cursor.sql:59`)
  and `lock_timeout` 2s per function.
* No backfill, no index build and no heavy IO from these three. Brian's IO stop rules
  (`docs/packets/claim-bootstrap-org-index-packet-20261108.md:317-325`) apply to window 1 as a
  whole, not because of these slots.

### 2.3 The bootstrap role: the window 1 abort risk (pre-check before window 1)

043870's role-check block (`20261105043870_buffered_search_feed_cursor.sql:90-105`) refuses with
**42704** if the feed role is absent, and with **55000** if it is elevated or has a member. That
**aborts window 1**, because the window is one owner-apply.

* The NOLOGIN role is created by `supabase/bootstrap/session_ingest_roles.sql:55-70`. That
  creation was added to the bootstrap on 2026-11-08 (commit 4475c6bf).
* The prod owner profile sets `HOOPDB_ROLE_BOOTSTRAP=false` and `ROLE_VERIFY=true`
  (`src/hoopdb/owner-operator-apply.ts:100-108`), so the migrate run will **not** create it.
  The role must already exist. The phase-2 verifier runs on every migrate (`src/hoopdb/migrate.ts:66,336,370`).
* **Whether the role exists in prod: unmeasured.** Brian, before window 1, as a counts-only read:

```sql
SELECT count(*) AS present,
       count(*) FILTER (WHERE rolcanlogin) AS login,
       count(*) FILTER (WHERE rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls) AS elevated
FROM pg_roles WHERE rolname = 'buffered_search_feed_runtime';
SELECT count(*) AS members FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid
WHERE r.rolname = 'buffered_search_feed_runtime';
-- need: present = 1, elevated = 0, members = 0
```

* If `present = 0`: creating the role (running `session_ingest_roles.sql`, or that file's
  NOLOGIN create only) is a role change and needs **Khaliq's separate OK**. Do this before
  window 1, or window 1 aborts at 043870 with 42704. The runbook describes these failures at
  `runbooks/session-directory-search-worker-bootstrap.md:433,454-466`.

### 2.4 LOGIN vs NOLOGIN

* **NOLOGIN is enough for window 1 and for the reader gate.** 043870 accepts the bare NOLOGIN
  role, and so does `supabase/bootstrap/buffered_search_feed_runtime_verify.sql:1-30`
  (which accepts the bare NOLOGIN or the LOGIN timeout profile, and requires the 3 RPCs).
* **LOGIN is only needed for the feeder.** `supabase/bootstrap/buffered_search_feed_runtime_role.sql:1-17`
  is operator-only, run with Khaliq's OK in his own words. It runs
  `ALTER ROLE … LOGIN NOINHERIT` with `statement_timeout` 5s, `lock_timeout` 2s and
  `idle_in_transaction_session_timeout` 10s. The password is set separately, from 1Password in
  memory, never from a file. Run the verify file before and after.
* The Lambda secret fields for this role are "validated only when INGEST_BUFFERED_SEARCH_FEED=on",
  and "no deploy wiring sets it" (`src/session-pipeline/lambda.ts:95-102`). The secret update
  procedure is `runbooks/sessions-prod-session-pipeline.md:102-118`. A warm Lambda keeps the old
  secret, so force a refresh.
* **Do LOGIN as late as possible**: just before the feeder enable, not at window 1. This keeps
  the window in which a login-capable role exists with no consumer as short as possible.
* Rollback: `supabase/bootstrap/buffered_search_feed_runtime_role_nologin.sql:1-25` returns the
  role to NOLOGIN, sets PASSWORD NULL and resets the timeouts. Turn the feed flag off and remove
  the secret fields first. Pooled connections persist after the role change.

### 2.5 Canary (counts only)

```sql
SELECT count(*) FROM sessions.__96bulls_migrations WHERE name IN (
  'upstream:20261105043860_buffered_search_gate.sql', 'upstream:20261105043870_buffered_search_feed_cursor.sql',
  'upstream:20261105043890_buffered_search_gate_retired_witness.sql');      -- expect 3
SELECT to_regprocedure('sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)') IS NOT NULL; -- t
SELECT count(*) FROM sessions.buffered_search_feed_cursors;               -- 0 before the feeder
```

Then run `buffered_search_feed_runtime_verify.sql` (pass/fail only).

### 2.6 Rollback

**Never run a down migration on a real database.** The down files (`supabase/rollbacks/`) are
code-only. Each one says to run it with `psql -1 -v ON_ERROR_STOP=1`, and that a hosted DB
needs the user's OK. **They delete ledger rows**: 043860.down at `:22-26`, 043870.down at
`:31-35`, and 043890.down at `:213-214`. A deleted ledger row with its objects gone moves the
window state and the generation. 043870.down also drops the cursors table, so cursor positions
are lost (`:25-28`). 043890.down refuses if any retired claim exists (`:15-18`): "Turn
READ_API_BUFFERED_SEARCH_GATE off instead". The real-DB rollback for every slot here is
therefore **to switch off the consumer**: reader gate off (section 3) and feeder off
(section 4). The functions stay in place, inert.

## 3. Reader deploy with READ_API_BUFFERED_SEARCH_GATE

* **Mechanism.** `.github/workflows/deploy-session-read-worker.yml` `workflow_dispatch`
  (`:66-67`) with input `buffered_search_gate=on` (`:144`). `render-deploy-config.ts` renders
  the var `"on"` plus the `HISTORY_CONTROL_HEAD` service binding to
  `relayhistory-api#SessionReadPrivacyControls`
  (`packages/session-read-worker/scripts/render-deploy-config.ts:28-29,67,78-85,207-208,230-245,265,274-282`).
  The parser accepts only `on`, case- and whitespace-insensitive (`src/session-read-api/buffered-search-gate.ts:99-102`).
* **The workflow states this itself** (`deploy-session-read-worker.yml:50-61`): it "must be
  deployed BEFORE the buffered search feeder"; turning it on "is a production change: dispatch
  is Khaliq's call"; and "every later dispatch without the input turns the gate off again". So
  every later reader deploy must pass the input, until the default changes. Brian is the only
  reader deployer, which keeps this manageable.
* **Prerequisites.** The CH columns (section 1); without them, gate mode returns 503. 043860
  applied (window 1); without it, every gate call fails closed and every buffered hit drops
  (`buffered-search-gate.ts:63-67`). That is safe but useless. The control-head binding must
  resolve; without it the Worker logs a warn and buffered rows drop
  (`packages/session-read-worker/src/index.ts:125-130,412-417`).
* **Behaviour with the gate on and no feeder yet.** Canonical rows pass. When there are no
  buffered refs, `filter` returns early, with no control-head call and no SQL
  (`buffered-search-gate.ts:419-421`). So enabling the gate before the feeder costs only the
  wider column list. Measured cost: none. Expected cost: negligible, unmeasured.
* **Behaviour with buffered rows.** The gate is fail-closed (`buffered-search-gate.ts:1-74`):
  rule d drops deleted sessions, rule f drops resets, an unknown origin drops (`origin_unknown`),
  there is a 2 s control-head deadline (`:80`), at most 16 owners (`:82`) and at most 500 hits
  (`:245-254`), and one READ ONLY transaction per owner (`:280-318`). A portable (non-CH)
  adapter with the gate on throws "buffered search gate requires the ClickHouse binding"
  (`page.ts:162-163`).
* **Secrets.** The Worker's CH secrets come from `wrangler secret put` via 1Password
  (`deploy-session-read-worker.yml:63-65`). This step changes no secret.

**Canary.** Telemetry `gatedPage` exposes two counts (`src/session-read-api/app.ts:544-548`).
Watch the read API's 5xx rate and `search_timeout` rate for GET `/v1/events?q=` (`app.ts:799`)
and `/v1/sessions/:id/events` (`app.ts:705`) against a baseline taken before the deploy. Expect
no change.

**Rollback.** Redeploy without the input; the gate goes off (`deploy-session-read-worker.yml:50-61`).
Khaliq's OK is needed for the redeploy. If the feeder is running, stop the feeder first: with
the gate off, canonical mode still filters `origin = 'canonical'`, so buffered rows are hidden,
not exposed (`page.ts:169-185`).

## 4. Feeder deploy / enable

**Two separate steps, each with its own Khaliq gate.**

1. **Rollout: creates the rule DISABLED.** `bufferedSearchFeedScheduleProps` uses
   `rate(1 minute)`, with state `DISABLED` unless `bufferedSearchFeed.enabled` is true, and
   Input `{"bufferedSearchFeed":true}` (`stacks/aws/session-pipeline.ts:467-476`). Plan phases
   "session buffered search feed rollout" / "…update" are their own reviewed phase; any other
   combination is refused (`stacks/aws/transcript-store.ts:421-441`). The runbook confirms
   that the rollout creates only the DISABLED rule (`runbooks/sessions-prod-session-pipeline.md:258-269`).
   **Whether this rollout phase has already been applied in prod: unverified.**
2. **Enable.** Config `transcriptStore.sessionPipeline.bufferedSearchFeed = {enabled: true,
   gateDeclared: "buffered-search-gate/v1"}`. The stack refuses `enabled` without the exact
   `gateDeclared` ("deploy and verify the read gate first") and without `searchIndex: true`
   (`stacks/aws/session-pipeline.ts:183-196`). It then sets env `INGEST_BUFFERED_SEARCH_FEED=on`
   and `BUFFERED_SEARCH_FEED_GATE_DECLARED` (`:346`), and enables the rule. The runtime
   re-checks the same contract (`src/session-pipeline/buffered-search-feed.ts:113-133`).
   **Blocked by G1-G4 (section 7).**

Runtime facts:

* It runs on the session pipeline Lambda: nodejs24.x, arm64, 3008 MB, timeout 300 s
  (`stacks/aws/session-pipeline.ts:374-391`). No reserved concurrency was found. It is a
  separate scheduled invocation, not on the ingest or manifest hot path. It does share the
  function's concurrency and the CH cluster.
* It uses its own `pg.Pool` (max 1, idle 60 s), and is CH-only: a portable search backend
  logs `clickhouse_search_required` and builds no feed (`src/session-pipeline/lambda.ts:382-416`).
* Handler: if the flag is not `on`, it returns `ok:true` and does nothing. A gate refusal
  returns `ok:false`. The run log has `listed`, `inserted`, `skipped`, `count`, `deleted`, `ok`,
  `state` and `reason` (`lambda.ts:679-701`).
* Defaults: page 50 (1-200), rate 5 stages/s (1-50), budget 50 s (5-240 s), since = cutover,
  cursor `backfill-20261105` (`buffered-search-feed.ts:81-82,134-150`). **The stack exposes
  none of these knobs** (`session-pipeline.ts:183-196,346`), so prod runs the defaults (G6).
* The page RPC lists only `receipt_state='acknowledged'`, `cleanup_state='live'` stages that are
  the active outbox stage, keyset on `(created_at, stage_token)`
  (`20261105043870_buffered_search_feed_cursor.sql:150-166`). This is the late-ack gap (G2).

**Stop (immediate).** Disable the rule. This is the analog of the runbook's disable-rule stop
(`runbooks/sessions-prod-session-pipeline.md:254,298`). It is reversible and does not touch the
cursor. A stack update to `enabled: false` is the durable stop. Both need Khaliq, except that
an emergency stop is already covered by the runbook's stop table.

## 5. Backfill since 2026-11-05 05:33:45Z

* **No new code and no new command.** The backfill *is* the feeder's first runs: cursor
  `backfill-20261105`, bound to `since` = the cutover. The page RPC refuses a cursor/since
  mismatch with 22023 (`20261105043870_buffered_search_feed_cursor.sql:150-166`). It is
  **global across all tenants**; the page RPC has no tenant filter. It is backend-agnostic up
  to the sink: the PG cursor, the S3 stage read and the projection (`buffered-search-feed.ts:275-316`)
  produce neutral `IndexRow`s. Only the write (`:409-448`) is CH.
* **Window-start skip (G7).** The feeder drops rows with `ts` before the CH window start
  (default 14 days, `src/session-pipeline/reconcile.ts:47,112-119`, or the CH state value), and
  still advances the cursor (`buffered-search-feed.ts:428,461,494`). With the 14-day default,
  events from the cutover day start being skipped around **2026-11-19**. If the enable slips
  past that, the earliest post-cutover sessions are never indexed. The CH window state value in
  prod: unmeasured.

### 5.1 Estimates (arithmetic shown)

Assumptions: defaults (page 50, 5 stages/s, budget 50 s); one invocation per minute; per-stage
latency (SQL plus S3 read plus projection) is lower than the rate limit. That last point is
**unmeasured**, so every figure below is a best case.

* Stage throughput ceiling: 5 stages/s × 50 s = **250 stages/min = 15,000 stages/h**.
* **"About 167k" as stages:** 167,000 ÷ 250 = 668 min ≈ **11.1 h**.
* **"About 167k" as CH rows:** stages = 167,000 ÷ R, where R (rows per stage) is
  **unmeasured**. At R = 10: 16,700 stages ÷ 250 ≈ 67 min. At R = 100: 1,670 stages ≈ 7 min.
  Which unit "167k" counts is itself unverified.
* **Global (all tenants):** the total stage count since the cutover in prod is **unmeasured**.
  Hours = N_stages ÷ 15,000. Brian's counts-only pre-read:

```sql
SELECT count(*) FROM sessions.ingest_stage_intents
WHERE created_at >= '2026-11-05T05:33:45Z'
  AND receipt_state = 'acknowledged' AND cleanup_state = 'live';
```

  This read uses the same filters as the page RPC, except the active-outbox condition. Run it in
  a quiet period; its plan and IO are unmeasured.
* **CH insert pattern:** one batched insert per page of 50 stages, split further above 5,000
  rows or 64 MiB (`buffered-search-feed.ts:76-83,409-448`). That gives about 5 inserts/min,
  each about one new part in the November partition (`toYYYYMM(ts)`). This is well under CH's
  usual too-many-parts thresholds, but the cluster's actual part and merge headroom is
  unmeasured.
* **CH insert throughput: unmeasured.** The only number to hand is about 1,600 rows/s bulk on a
  small 3-node OpenSearch cluster. That is a **portability bound for a future sink only, not a
  CH number**: 167,000 ÷ 1,600 ≈ 104 s. It shows that the rate limiter (250 stages/min), not
  the sink, would bound a future backend too, unless R > ~384 (1,600 × 60 ÷ 250).

### 5.2 Stop rules (CH, plus Brian's PG rules)

Take a baseline before the enable. Stop (disable the rule) if any of these trips:

* **Parts:** active parts per partition for `transcript_events` keeps rising across 3 runs, or
  exceeds 2× baseline.
  `SELECT partition, count() FROM system.parts WHERE table='transcript_events' AND active GROUP BY partition`
* **Merges:** `SELECT count() FROM system.merges WHERE table='transcript_events'` stays above
  baseline for more than 10 min.
* **Mutations:** any `system.mutations` row with `is_done = 0` for the table (the feeder's
  `deleteBufferedRows` and the indexer's `erase` both issue lightweight deletes,
  `buffered-search-feed.ts:344-363`, `src/session-pipeline/search-index.ts:372-382`).
* **Insert rate / errors:** any run log with `ok:false`, or `inserted` per minute well above
  the 250-stage ceiling times the measured R (`lambda.ts:679-701`).
* **Read API:** the 5xx or `search_timeout` rate on `/v1/events?q=` rises above baseline
  (section 3).
* **PG (Brian's IO stop rules, `claim-bootstrap-org-index-packet-20261108.md:317-325`):**
  `pg_stat_activity` IO waits, provider IO utilisation, ingest p95 claim latency, and the
  55P03 and 57014 counts. The feed role's 5 s statement timeout and 2 s lock timeout bound
  each call (section 2.4).

The thresholds above are starting proposals, not derived from measurements.

## 6. Canary and rollback per step

All queries are counts only. Never select tokens, ids, cursors or content.

| Step | Canary | Rollback (real DB / prod) |
|------|--------|---------------------------|
| 1 CH DDL | `system.columns` returns 2 rows with the right types and DEFAULT; origin counts are only `canonical` (section 1) | none needed; never drop while the gate is on |
| 2 role pre-check | `present=1, elevated=0, members=0` (section 2.3) | n/a (read) |
| 3 window 1 | 3 ledger rows; the function is present; cursors count = 0; role verify passes (section 2.5) | **no down migration**: the down files delete ledger rows (section 2.6). Switch off the consumers instead |
| 4 reader gate | `gatedPage` counts; 5xx and `search_timeout` at baseline (section 3) | redeploy without `buffered_search_gate` (gate off) |
| 5 role LOGIN | verify file passes before and after (section 2.4) | `buffered_search_feed_runtime_role_nologin.sql`, after the flag is off and the secret fields are removed |
| 6 feeder rollout | the rule exists and is DISABLED | stack update removing it (Khaliq) |
| 7/8 feeder enable and backfill | run log `ok:true`; `listed`/`inserted`/`skipped` counts; `SELECT stages_indexed, stages_skipped, updated_at FROM sessions.buffered_search_feed_cursors` (never the positions); `SELECT origin, count() FROM transcript_events WHERE ts >= '2026-11-05 05:33:45' GROUP BY origin`; the #385 census (section 7, G1) | disable the rule. Buffered rows stay hidden from readers if the gate is turned off (canonical mode filters them). Deleting them is a CH lightweight delete scoped by fingerprint prefix `bf`; that needs Khaliq, and should be done only if the gate cannot hold |

## 7. Code gaps

Each gap lists its files, whether it blocks the feeder enable, a suggested default-off flag, and
the backend-change marker.

1. **G1: version order (MUST, blocker).** Buffered `version` = `received_at` in ms
   (`buffered-search-feed.ts:278-279,309`). Canonical `version` = the writer's wall clock. In a
   ReplacingMergeTree(version) with the same identity, a buffered row with a higher or tied
   version replaces the canonical row at merge time, and a canonical-only filter cannot bring it
   back. The header claim "any later canonical pass … wins" (`buffered-search-feed.ts:27,40`) is
   disproved by #382's DEFECT cases (canonical v1 then buffered v2, a tie, clock skew, and a
   later-received stage replacing canonical). #382's d3 case shows that feeder cleanup leaves
   **no** row. Fix: #385's S encoding (`version_b = floor(epoch_seconds(stage.created_at))`,
   below the canonical minimum, plus a PG same-second refusal). Not yet code. Flag
   `INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION` (named in #385). #385's section 10
   prerequisites:
   * a counts-only census: canonical count > 0, `countIf(version=0)=0`,
     `countIf(version<1e12)=0`, buffered count = 0;
   * a shared canonical-version guard (relayhistory-cloud owner lane);
   * a tie-frequency measurement;
   * a TieRefusedOutstanding gauge plus an alarm (a cloud resource: Khaliq);
   * expiry-to-terminal tests;
   * the flag flip on Khaliq's word.

   **PRs:** #382 advisable (the regression base, plus the `default_kind` check); #385 not
   superseded (the design of record). **Backend:** the S encoding is WASTED (specific to
   ReplacingMergeTree); the PG same-second refusal carries over.
2. **G2: late-ack skip (MUST, blocker).** The page RPC lists only stages already acknowledged,
   keyset on `(created_at, stage_token)` (`20261105043870_buffered_search_feed_cursor.sql:150-166`).
   A stage acknowledged after the cursor has passed it is skipped forever (#385 Q7/R2). This
   needs a durable revisit. Landed migration bytes are frozen, so the fix is a new slot or
   code-side, which is outside window 1. Suggested flag
   `INGEST_BUFFERED_SEARCH_FEED_LATE_ACK_REVISIT`. **Backend:** carries over (PG-side).
3. **G3: erasure lane (MUST, blocker), PR #383.**
   `src/session-pipeline/buffered-erasure-delete.ts` (new), with flag
   `INGEST_BUFFERED_ERASURE_DELETE=off|on` and schedule `{"bufferedErasureDelete":true}`. It
   needs no new migration; it uses the 043870 `stage` RPC and keeps its cursor in
   `search_ingest_state`. Knobs: page 1-200 (default 25), rate 1-20 (default 2), budget
   5,000-240,000 ms (default 40,000). The runbook forbids arming without it
   (`runbooks/sessions-prod-session-pipeline.md:268-269`). It changes the feeder fingerprint to
   `bf<body>:<stageToken>:<ords>`, and pre-#383 rows count as `legacy_unmapped` and are never
   deleted. So it must land **before** the first feeder write.
   **Main without #383:**
   * The feeder's own cleanup deletes only its own superseded rows (`buffered-search-feed.ts:344-363`).
   * The indexer's delete pass skips `bf` rows that are absent from S3 (`search-index.ts:318-340`).
   * `erase` (`search-index.ts:372-382`) deletes every origin for a scope, but only runs inside
     a canonical indexing pass for that scope. Those passes are driven by manifest
     notifications and reconcile, and whether a buffered-only session ever gets one is not
     derivable from code.

   So an erased session's buffered rows **physically remain in CH**. They are **not searchable
   through this repo's read API**: the gated Worker drops them (rules d and f, fail closed,
   `buffered-search-gate.ts:1-74`), and the ungated reader filters `origin='canonical'`
   (`page.ts:169-185`). But any other reader of `transcript_events` without an origin filter
   would serve them (G4). Whether #383's gone/refused mapping covers a user-initiated session
   delete or reset (as opposed to GC retire/redact, a GC lease refusal or a batch leaving the
   buffer) is unverified. **Backend:** the lane logic and the PG `stage` check carry over; its
   CH delete is WASTED.
4. **G4: other CH readers (verify; blocker until verified).** This repo's reader filters by
   origin. Whether any other consumer of `transcript_events` (for example the relayhistory-cloud
   side) reads without an origin filter is **unverified** and not derivable from this repo.
   Such a reader would show buffered rows that were never gated, including erased ones.
   No flag; this is a verification. **Backend:** n/a.
5. **G5: no sink abstraction (follow-up).** `BufferedFeedDeps` holds `ch: ClickHouseClient`
   directly (`buffered-search-feed.ts:367-379`). There is no sink interface, unlike
   `SearchProjectionIo` for the canonical path (`src/session-pipeline/search-adapter.ts:57-60`).
   The projection/sink split exists in the function boundaries (projection at `:275-316`; CH
   at `:324-363,381-390,409-448`) but not as a type. Follow-up: a `BufferedFeedSink` interface
   mirroring `SearchProjectionIo`, with no behaviour change, so no flag is needed. Do **not**
   add new CH SQL beyond the existing writer (avoid or defer). **Backend:** carries over (it
   is the portability seam).
6. **G6: stack knobs not exposed (follow-up, config only).** PAGE, RATE, BUDGET_MS, SINCE and
   CURSOR are parsed (`buffered-search-feed.ts:134-150`), but the stack accepts only `enabled`
   and `gateDeclared` (`stacks/aws/session-pipeline.ts:183-196,346`). Prod cannot slow or speed
   the backfill without code. Suggested: optional stack settings that default to absent
   (= the code defaults), so no new flag. **Backend:** carries over.
7. **G7: window-start skip (risk / follow-up).** See section 5. Rows before the CH window start
   are dropped and the cursor still advances (`buffered-search-feed.ts:428,461,494`;
   `reconcile.ts:47,112-119`). This becomes time-sensitive from about 2026-11-19. Suggested
   flag `INGEST_BUFFERED_SEARCH_FEED_IGNORE_WINDOW` (default off), or a one-off CH window
   state read before the enable. **Backend:** the skip is CH-state-specific; WASTED.
8. **G8: no keep-up trigger beyond the schedule (follow-up).** Bucket notifications fire only
   on `/manifest.json` (`stacks/aws/session-pipeline.ts:310-326`). Freshness of new buffered
   sessions therefore depends on the 1-minute schedule plus the 250-stage/min ceiling. After
   the backfill, end-to-end freshness is about 1-2 min (unmeasured), not the sub-second CH
   figure. This is acceptable for the fast fix. **Backend:** carries over.
9. **G9: no alarm on `ok:false` or blocked runs (follow-up; a cloud resource, so Khaliq).**
   The run log carries `ok`, `state` and `reason` (`lambda.ts:679-701`), but no alarm is
   defined in the stack. Until one exists, Brian watches the logs during the backfill.
   **Backend:** carries over.
10. **G10: `default_kind` hardening, #382 (advisable).** Main's `assertOriginColumns`
    (`buffered-search-feed.ts:324-333`) checks name and type only, so a MATERIALIZED or ALIAS
    column passes. #382 adds `kind: "DEFAULT"`. With section 1's DDL the kind is DEFAULT
    anyway, so this does not block. CH-only: small, acceptable, CH remains incumbent.
11. **G11: role presence before window 1 (operator pre-check, blocker for window 1, not a code
    gap).** See section 2.3.

## 8. Order-of-operations checklist for Brian's session

`[K]` = needs Khaliq's own explicit OK in his own words in this session, for this step.

1. [ ] Baselines: read API 5xx and `search_timeout`; CH `system.parts`/`system.merges`/`system.mutations`
   for `transcript_events`; PG IO, ingest p95, and the 55P03/57014 counts.
2. [ ] Role pre-check (section 2.3), counts only. If absent: **[K]** create it NOLOGIN, before
   window 1.
3. [ ] **[K]** CH DDL (section 1). Canary: `system.columns`. Watch the reader for an
   `origin_column_changed` blip.
4. [ ] **[K]** Window 1 per its own plan: the 043850..044030 owner-apply under the accepted
   outage. Canary section 2.5. Apply Brian's IO stop rules.
5. [ ] **[K]** Reader deploy with `buffered_search_gate=on` (section 3). Canary: 5xx and
   `search_timeout` at baseline. From now on, every reader dispatch passes the input.
6. [ ] **[K]** Feeder rollout phase (DISABLED rule), if it is not already in prod (section 4).
   Canary: the rule is DISABLED.
7. [ ] **STOP HERE until G1, G2, G3 have landed on main and G4 is verified.** #383 must be on
   main before any feeder write.
8. [ ] #385 section 10 census (counts only) on CH. Proceed only if it passes.
9. [ ] **[K]** Role LOGIN plus the password from 1Password in memory; verify before and after
   (section 2.4). **[K]** Secret fields update and force a Lambda refresh
   (`runbooks/sessions-prod-session-pipeline.md:102-118`).
10. [ ] **[K]** Enable the erasure lane (`INGEST_BUFFERED_ERASURE_DELETE=on`, per #383) and
    the version fix flag (`INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION=on`, per G1).
11. [ ] **[K]** Feeder enable (`bufferedSearchFeed.enabled: true`, `gateDeclared:
    buffered-search-gate/v1`). The backfill starts on the next minute.
12. [ ] Watch the first 3 runs: run log counts, cursor counts, origin counts, and the section
    5.2 stop rules. Stop = disable the rule.
13. [ ] Report: per step pass/fail, durations, counts (`listed`/`inserted`/`skipped`, the cursor's
    `stages_indexed`/`stages_skipped`, origin counts), and the stop-rule readings. No ids, tokens,
    hosts or content.
