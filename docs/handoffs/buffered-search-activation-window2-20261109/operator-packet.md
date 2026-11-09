<!-- Operator recipe for hosted buffered search activation, window 2: the eight pending steps 044050..045030, applied under
     the live 044030 generation overlap (zero-503 §3.2), followed by the list fix, the reader gate and the feeder.
     Written 2026-11-09 by layered-sessions-pipeline. Source-only: nothing here was run against any database,
     Worker, Lambda or ClickHouse cluster. Production identifiers are written as tags or `<id>`.
     Durations are untested estimates. Items marked UNVERIFIED were not confirmed in code. -->
# Operator recipe: buffered search activation, window 2 (044050..045030) under the live 044030 overlap (96Bulls)

**Nothing in this document is pre-approved.** Every apply, deploy, flag flip, role change, secret change and
workflow dispatch below needs Khaliq's own OK, in his own words, for that step. The Executor
(brian-db-takeover-linuyc) carries each step out (AGENTS.md: "Deploys, cloud resources, credentials, IAM changes,
and applying migrations to any real database still need Khaliq's separate, explicit OK").

Summary of the sections:
- §0 Window 2 is **8 steps, 044050..045030**, at the current main pin. 045040 is in the next window.
- §1 Source pin, the eight file digests, and how to compute H.
- §2 Role pre-checks (045000, 045030) and the §2a-style boundary re-bootstrap from the new pin.
- §3 ClickHouse DDL (two columns) and the `system.columns` check.
- §4 The window: Worker {G,H} deploy, overlap open, migrator, verify, rebind, G2 bulk re-proof, Worker cleanup.
- §5 List fix (044080/044090): writer enable, backfill, sentinel, reader enable.
- §6 Reader gate deploy inputs.
- §7 Feeder: hard code preconditions, the #448 knobs, and backfill sizing (feed packet §5.1 arithmetic).
- §8 Canary and rollback per step (counts only).
- §9 Order-of-operations checklist (gate owner and actor on every line).

Inputs this packet builds on (read them; this file does not repeat their reasoning):
- Plan A recipe: docs/handoffs/plan-a-044040-execution-recipe-20261109.md (§2 env, §2a boundary, §5 rebind, §6 G2, §8 rollback).
- Zero-503 design: docs/design/zero-503-schema-windows.md §3.2 (every later window G → H) and §4 (limits).
- List fix: docs/handoffs/ingest-claim-sessions-list-fix-20261109/operator-packet.md §1-§5.
- Feed: docs/handoffs/buffered-search-feed-activation-20261109/operator-packet.md §1, §3, §4, §5, §7, §8.

Citations are `path:line` at the pin ce9b4464 unless stated.

## 0. Which steps are in window 2

- Window 2 = **8 steps: 044050, 044070, 044080, 044090, 044100, 045000, 045010, 045030**. These are exactly the
  migration files added between the plan-A pin 3d0b4191 and ce9b4464
  (`git diff --name-status 3d0b4191 ce9b4464 -- supabase/migrations` shows 8 `A` lines). There is no 044060
  and no 045020 file on main.
- **045040 is not in this window.** It is PR #456 (still OPEN at the time of writing); its file would be
  `supabase/migrations/20261105045040_selfhost_output_facts.sql`. It belongs to the next window, unless #456
  merges before the pin is chosen; then the pin, the step list, H and the digests in §1 must all be redone.
- The migrator has no target-slot option. It applies every pending file in the checkout it runs from, so it
  **must run from ce9b4464** (or from a re-derived pin, per the line above).
- Precondition: the plan-A window is complete. The ledger tail is `upstream:20261105044040_buffered_stages_session_first.sql`,
  the marker is PASS for G_cur (the plan-A G_new), and the 044030 overlap tables exist.

## 1. Source pin, digests and H

Pin: `ce9b4464ff16fad52d8507706356b7a4aebd59c3` (origin/main when this packet was written).
```sh
git clone -q https://github.com/AgentWorkforce/relayhistory-pipeline.git <scratch>/window2
cd <scratch>/window2 && git checkout -q ce9b4464ff16fad52d8507706356b7a4aebd59c3
git rev-list --first-parent origin/main | grep -q '^ce9b4464' && echo first-parent-ok
git diff --name-status 3d0b4191 ce9b4464 -- supabase/migrations     # expect: exactly the 8 A lines below
ls supabase/migrations/ | sed -n '/20261105044050/,$p'               # expect: the 8 files below, last = 045030
sha256sum supabase/migrations/2026110504{4050,4070,4080,4090,4100,5000,5010,5030}_*.sql
git status --porcelain                                               # expect: empty
bun install --frozen-lockfile
```

| step | file (`upstream:` + name) | sha256 at ce9b4464 | rollback file |
|---|---|---|---|
| 044050 | 20261105044050_selfhost_erase_short_summary_provenance.sql | e89d42177fe7aacf871161a36a442005366b8e260c23502f60683df76dcfaa7a | yes |
| 044070 | 20261105044070_selfhost_erase_search_text_rebuild.sql | 1fb6ba016733036b43d42dfd6c0a2331566fcc7d18ee215d34d4085f2c0f1e28 | yes |
| 044080 | 20261105044080_ingest_claim_sessions.sql | 1cb64d0241e4e3f75149a1fd4ffde81365f3fc4a98a36eccf44f642076b335ba | yes |
| 044090 | 20261105044090_claim_sessions_reader.sql | 047846f6cd640752537e66e074a00d87f0e2d9a6dc4c4c741c86468aad85f35a | yes |
| 044100 | 20261105044100_session_brief_summary_read.sql | 7a437ddba3600f2261f5436caa78f4daa8312f95298b58720c94638dfb038ccd | yes |
| 045000 | 20261105045000_digest_narrative_cache.sql | 3e87dd4b035a4327b29d1f52fa1f0ad970a0b4d852a4196d8b2995a950ba8c30 | yes |
| 045010 | 20261105045010_buffered_search_feed_same_second.sql | ab9856b0dd70bf07deee1b734a89107df82e144742f020da2dbabeda274b5d7b | yes |
| 045030 | 20261105045030_session_consumer_project_text_grant.sql | 2ec92c1ed9ce10eddf61dd60450d35ae37b68e2325405f3eb320501c87c03f38 | yes |

"rollback file" = `supabase/rollbacks/<name>.down.sql` exists at the pin. It is not a plan to run it (§8).

What the steps do, in one line each (from their headers): 044050 and 044070 replace two erase/retire functions
(provenance clear, search_text rebuild); 044080 adds the empty side table, its policy row and triggers, all flags
off; 044090 adds the reader functions and the policy reader CHECK, flags off; 044100 adds
`sessions.session_brief_summary_for` (SECURITY DEFINER); 045000 adds the digest narrative cache and two SECURITY
DEFINER functions and checks two roles; 045010 replaces `sessions.buffered_search_feed_stage(jsonb)` (adds the
`sameSecond` key); 045030 grants EXECUTE on `sessions.session_directory_project_text` to `search_consumer_runtime`.
None builds an index on a populated table (044080:36 "plain CREATE INDEX on a new, empty table"), so window 2
does **not** set HOOPDB_ALLOW_BLOCKING_INDEX_BUILD. 044080, 044090 and 045010 set `SET LOCAL lock_timeout='5s'`.

Generations:
```sh
G_CUR=<generation of the PASS marker, read live>   # expected: the plan-A G_new,
                                                   # 387f83b04f4bf5bc842cf6ddc21e475934d708922f079e7dcd3f08dd45499eff
H=$(bun src/hoopdb/migrate.ts --generation)        # from the ce9b4464 checkout; record the 64-hex value
```
If the live PASS generation differs from the plan-A G_new, stop: the plan-A window did not finish as recorded.

## 2. Role pre-checks and the boundary re-bootstrap (before the window)

### 2.1 Role pre-checks (read-only, as postgres or the owner)
```sql
SELECT rolname, rolcanlogin, rolinherit, rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb OR rolreplication AS elevated
  FROM pg_roles
 WHERE rolname IN ('bulls96_read_api','bulls96_digest_narrative','search_consumer_runtime') ORDER BY rolname;
-- expect 3 rows; elevated=false and rolinherit=false for bulls96_read_api and bulls96_digest_narrative
SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member
 WHERE r.rolname = 'bulls96_digest_narrative';   -- expect 0
```
- 045000 fails closed if `bulls96_read_api` or `bulls96_digest_narrative` is absent (42704) or elevated or
  INHERIT (55000) (20261105045000_digest_narrative_cache.sql:39-55). The migrator does not create it here,
  because HOOPDB_ROLE_BOOTSTRAP=false.
- 045030 fails closed 55000 if `search_consumer_runtime` is absent (20261105045030_session_consumer_project_text_grant.sql:37-39).
- If `bulls96_digest_narrative` is missing, the Executor runs the role bootstrap as postgres (a role change:
  Khaliq's OK). Its new block (after the `bulls96_read_api` block) creates the bare NOLOGIN NOINHERIT role and
  is idempotent; it refuses 55000 on an elevated or member role and changes nothing.
```sh
psql -v ON_ERROR_STOP=1 -X -d <the HoopDb database> -f supabase/bootstrap/session_read_api_roles.sql
```
  On PostgreSQL 16+ the postgres path checks `hoopdb.db_mode = 'supabase'` or a superuser `supabase_admin`
  (the block's create check). If it refuses 42501, add `PGOPTIONS='-c hoopdb.db_mode=supabase'` (UNVERIFIED
  whether production needs it). Rerun the pre-check; expect the row present, `rolcanlogin=false`.
- `supabase/bootstrap/digest_narrative_role.sql` (LOGIN for the narrative Worker) is post-migrate, operator-only
  and **not needed for search activation**. It is out of scope here.
- If `search_consumer_runtime` is missing, stop and ask; its provisioning is not part of this packet.

### 2.2 Boundary re-bootstrap from ce9b4464 (same procedure as plan A §2a)
Why: the event trigger `hoopdb_boundary.sessions_function_boundary()` embeds a frozen manifest and a frozen
SECURITY DEFINER allowlist. A trigger bootstrapped from 3d0b4191 refuses 42501 at the first new SECURITY
DEFINER function of window 2 (044100 `session_brief_summary_for`; 045000 `digest_narrative_read` and
`digest_narrative_upsert`) and the 045030 grant (not in the old manifest). The pin's bootstrap file adds those
three to the allowlist (hoopdb_sessions_function_boundary_supabase.sql:135-136, :252) and the new manifest (:106).
This is the cause of the plan-A 42501; do not skip it.

Read-only check before (expect `false`, `bulls96_runtime`) and after (expect `true`, `bulls96_runtime`):
```sql
SELECT position('session_brief_summary_for(text,text,text)' IN p.prosrc) > 0 AS has_window2_fns
  FROM pg_proc p WHERE p.oid = 'hoopdb_boundary.sessions_function_boundary()'::regprocedure;
SELECT substring(p.prosrc from 'v_runtime text := ''([a-z0-9_]+)''') AS runtime_role
  FROM pg_proc p WHERE p.oid = 'hoopdb_boundary.sessions_function_boundary()'::regprocedure;
```
Run as postgres from the clean ce9b4464 checkout, after §2.1 and before §4.3 (runbooks/session-directory-search-worker-bootstrap.md:647-650):
```sh
psql -v ON_ERROR_STOP=1 -X -v runtime_role=bulls96_runtime -d <the HoopDb database> \
  -f supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql
```
- Idempotent (CREATE OR REPLACE of the body; the trigger is kept when event, tags and state match).
- Refuses 55000 on a `runtime_role` mismatch or a tag-set mismatch. `DROP EVENT TRIGGER` and a rerun is then an
  operator decision under Khaliq's word; do not improvise it.
- Evidence: the normalized prosrc sha256 must equal SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 at the pin,
  `a7a9edfeac4e158a38a962ddb9ef253c78fff7b256ad81282cf92c8913e20125` (src/hoopdb/migrate-guards.ts:1542-1543;
  at 3d0b4191 it was `42ee52a7...af59`). Optional: `HOOPDB_FUNCTION_BOUNDARY_MODE=event-trigger bun src/hoopdb/migrate.ts --activation-check`,
  expect `prosrc_exact=true`.
- The new body admits everything the old one did plus the window-2 entries, so running it before the Worker
  deploy does not affect the live G runtime. It is still production DDL: exclusive DB slot, Khaliq's word.

Bootstrap files changed since 3d0b4191 (8 files, 92+/3-): cloudsql_migration_roles.sql, digest_narrative_role.sql
(new), hoopdb_sessions_function_boundary_supabase.sql, rds_migration_roles.sql, session_consumer_role.sql,
session_consumer_role_verify.sql, session_read_api_role_verify.sql, session_read_api_roles.sql. On Supabase
only the two files above are needed for window 2. The others are other targets (Cloud SQL, RDS), post-migrate
LOGIN steps, or verify files (UNVERIFIED whether the production consumer role needs `session_consumer_role.sql`
rerun; check `git diff 3d0b4191 ce9b4464 -- supabase/bootstrap/session_consumer_role.sql` and ask before running it).

## 3. ClickHouse DDL (any time before §6; Khaliq's OK)

From docs/proposals/buffered-search-feed.clickhouse.draft.sql:7-8, run against the search database:
```sql
ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin LowCardinality(String) DEFAULT 'canonical';
ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin_claim_token String DEFAULT '';
```
Check (the reader probes the same columns, src/session-read-api/queries.ts:321, :342):
```sql
SELECT name, type, default_kind, default_expression FROM system.columns
 WHERE database = currentDatabase() AND table = 'transcript_events' AND name IN ('origin','origin_claim_token');
-- expect 2 rows: origin | LowCardinality(String) | DEFAULT | 'canonical'
--                origin_claim_token | String | DEFAULT | ''
SELECT origin, count() FROM transcript_events WHERE ts >= now() - INTERVAL 1 DAY GROUP BY origin;
-- expect one row, origin = 'canonical'
```
Metadata-only `ADD COLUMN` with a constant default; no mutation is expected (`SELECT count() FROM system.mutations
WHERE table='transcript_events' AND is_done = 0` stays at baseline). Rollback: leave the columns (inert while the
gate and feeder are off). Dropping them is a separate decision.

## 4. The window (zero-503 §3.2: G → H, G = G_CUR, H from ce9b4464)

### 4.1 Worker first: expectedGeneration = G_CUR, overlapGeneration = H
Same deploy path and inputs as plan A §5 (`sessionIngest.expectedGeneration` / `overlapGeneration`,
INGEST_EXPECT_GENERATION / INGEST_ACCEPT_GENERATIONS). No behaviour change while the database is at G.
Canary: ingest 2xx and 503 counts unchanged against the pre-deploy baseline.

### 4.2 Open the overlap, immediately before the apply
```sh
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-overlap.ts status
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-overlap.ts open \
  --from "$G_CUR" --to "$H" --ttl-seconds 3600 --approval-sha <sha256 of the window approval.json>
```
- Output: one JSON window record, or `{"status":"refused","code":...}` (exit 2 on validation).
- Readiness must still be PASS for `--from` at open time.
- TTL: 60..14400 s. 3600 s covers the apply (minutes; §4.3) plus verify, rebind and margin. If the window expires
  before the rebind, the fence fails closed (the old outage, never a widening).
- Do **not** pass `--admit-during-apply`: it needs a full-union rehearsal first (overlap CLI header :7-8, design §4).
  With it off, G uploads are refused during pending and admitted again at PASS (design :299).
- 55P03/57014 from the CLI are lock waits, not fence refusals; rerun `status` before retrying.

### 4.3 Migrator, from the ce9b4464 checkout
Environment as plan A §2, **without** HOOPDB_ALLOW_BLOCKING_INDEX_BUILD:
```sh
export HOOPDB_DB_MODE=supabase HOOPDB_SUPABASE_SHIM=false HOOPDB_ROLE_BOOTSTRAP=false HOOPDB_ROLE_VERIFY=true
export HOOPDB_RUNTIME_ROLE=bulls96_runtime HOOPDB_RUNTIME_ROLE_LOGIN=true
export HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke
HOOPDB_URL='<owner-direct-url from 1Password>?options=-c%20statement_timeout%3D15min' \
  bun src/hoopdb/migrate.ts 2>&1 | tee <scratch>/migrate-window2.log
```
The URL is read into memory only (never written to disk). The migrator sets no statement_timeout of its own; 15 min is this packet's proposal, not a measured value. Expected log: "role validation pending", then
"migration applied" **x8** (044050 ... 045030, name order), "role validation passed", "hoopdb migrations complete".
Duration: untested; all 8 steps are function, grant and empty-table DDL, expected seconds to a few minutes.
Lock waits are bounded by the 5 s `SET LOCAL lock_timeout` in 044080/044090/045010; 044050/044070/044100/045000/045030
set none and inherit the session default. Watch from a second session (plan A §4 query).

### 4.4 Verify
```sql
SELECT name, sha256 FROM sessions.__96bulls_migrations
 WHERE name > 'upstream:20261105044040' AND name LIKE 'upstream:2026110504%' ORDER BY name;
-- expect exactly 8 rows, 044050 ... 045030, sha256 as §1
SELECT count(*) FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:20261105045040%';  -- expect 0
SELECT generation, status FROM <role-validation-table> WHERE name = <marker-name>;  -- expect H, pass
SELECT write_enabled, read_enabled, backfill_cursor IS NULL, backfill_complete_at IS NULL
  FROM sessions.ingest_claim_sessions_policy;                                      -- expect false,false,true,true
SELECT count(*) FROM sessions.ingest_claim_sessions;                               -- expect 0
```
The ledger contiguity preflight (migrate-guards.ts:1340, `ledger_gap` :1370/:1380, `unledgered_schema` :1458)
runs before any step; a refusal there writes nothing. Never insert or mark ledger rows by hand.

### 4.5 Rebind (closes the overlap)
```sh
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-rebind.ts --from "$G_CUR" --to "$H"
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-rebind.ts --from "$G_CUR" --to "$H" --execute
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-overlap.ts status   # expect no open window
```
Counts-only JSON, or `{"status":"refused","code":...}` exit 1. Idempotent. The {G,H} Worker's next G call gets
55000, retries at H and is admitted.

### 4.6 G2 bulk re-proof (target generation H)
```sh
<RAM-only credential pipe named in Khaliq's word> | bun run scripts/owner-buffered-parity-operator.ts bulk \
  --expect-source-sha ce9b4464ff16fad52d8507706356b7a4aebd59c3 --expect-target-generation "$H" \
  --batch-size 25 --confirm-write owner-buffered-parity-bulk/v1
```
PASS = one JSON receipt with `"status":"complete"` and exit 0 (counts only). Other status exits 3, refusal 1.
Resume by rerunning the same command. Never relabel `target_generation`. Credentials never go in argv, env or a file.

### 4.7 Runtimes and Worker cleanup
- Every runtime that computes `expectedSchema()` (search worker, alleyoop ingest, indexer, consumers, buffered GC
  and publication) must be rebuilt from ce9b4464 (or later with no new migration) and redeployed after PASS.
  An image from 3d0b4191 reports `validation_pending` once PASS moves to H; an image with 045040 waits on
  `schema_pending` (plan A §5a; src/shared/db.ts schemaVerdict).
- Worker cleanup: deploy with expectedGeneration = H and no overlap (design §3.2 step 6).

## 5. List fix (044080 + 044090), by reference

Procedure, preconditions and stop rules: list-fix packet §2-§5. Order is fixed: writer → backfill → sentinel →
reader. Do not flip a flag while a backfill batch runs (it holds the policy row FOR UPDATE).

Writer enable (list-fix §2):
```sql
SELECT count(*) AS holders, max(now() - a.xact_start) AS oldest
  FROM pg_locks l JOIN pg_stat_activity a USING (pid)
 WHERE l.relation = 'sessions.ingest_claim_attempts'::regclass
   AND l.mode = 'RowExclusiveLock' AND a.pid <> pg_backend_pid();   -- proceed only if oldest < 1s
BEGIN; SET LOCAL lock_timeout = '2s';
UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true; COMMIT;
```
Backfill (list-fix §3; sizing read first, then `--dry-run`, then chunks of 60 batches; rerun resumes):
```sh
op read "op://<vault>/<schema owner>/url" | jq -Rc '{ownerUrl: .}' \
  | bun scripts/backfill-ingest-claim-sessions.ts --dry-run
op read "op://<vault>/<schema owner>/url" | jq -Rc '{ownerUrl: .}' \
  | bun scripts/backfill-ingest-claim-sessions.ts --batch-size 500 --statement-timeout-ms 30000 --max-batches 60
```
Duration: ceil(claims / 500) × per-batch time (list-fix §3 table; about 334 batches for the heavy user's ~167,000
claims alone, 1.4 to 28 min at 0.25 to 5 s per batch; the walk is global).
Sentinel (list-fix §4): `SELECT backfill_complete_at IS NOT NULL FROM sessions.ingest_claim_sessions_policy;`
expect `true`, plus the 044080 packet pair check (`expected_pairs = side_rows`, `stale_rows = 0`).
Reader enable (list-fix §4):
```sql
BEGIN; SET LOCAL lock_timeout = '2s';
UPDATE sessions.ingest_claim_sessions_policy SET read_enabled = true; COMMIT;
```
Stop rules (list-fix §2-§3): claim p95 up more than 20%, any rise in 55P03/57014, IO waiters, a batch over 10 s
(halve to 250 above 5 s).

## 6. Reader gate deploy inputs

- Workflow: `.github/workflows/deploy-session-read-worker.yml`, `workflow_dispatch` (feed packet cites :66-67) with
  input **`buffered_search_gate=on`** (:144). Contract `buffered-search-gate/v1`
  (src/session-read-api/buffered-search-gate.ts:94); the parser accepts only `on`, trimmed, any case (:99-102).
- **Every later dispatch must pass the input**; a dispatch without it turns the gate off (workflow :50-61).
- Preconditions: §3 columns present (else gate mode returns 503); 043860 applied (plan A); the control-head
  binding resolves. No secret changes.
- Canary: read API 5xx and `search_timeout` counts on `/v1/events?q=` and `/v1/sessions/:id/events`, against a
  pre-deploy baseline; expect no change (no buffered rows exist yet).
- Rollback: redeploy without the input (Khaliq's OK). If the feeder is running, stop it first.

## 7. Feeder enable

### 7.1 Hard preconditions (STOP until all hold)
The feeder **cannot be armed from main at ce9b4464**, independent of any OK:
1. **Version ceiling unpinned.** `BUFFERED_VERSION_CEILING` is `undefined`
   (src/session-pipeline/buffered-search-feed.ts:209). With SECOND_VERSION on, `bufferedFeedConfig` refuses
   `buffered_version_ceiling_unpinned` (test: stacks/aws/__tests__/session-pipeline.test.ts:404-408). It is pinned
   only in a later commit from the operator's counts-only CH census (buffered-search-feed.ts:200-208;
   docs/design/buffered-feed-version-encoding-s.md §1 and §10): for `origin='canonical'` `count()`, `min(version)`,
   `countIf(version = 0)`, `countIf(version < 1e12)`; for buffered `count()` and `max(version)`.
2. **Stack pass-through missing.** stacks/aws/session-pipeline.ts accepts only `enabled`, `gateDeclared`,
   `pageSize`, `stagesPerSecond`, `budgetMs` (:197; env :362-365). It sets neither
   `INGEST_BUFFERED_SEARCH_FEED_SECOND_VERSION` nor `INGEST_BUFFERED_SEARCH_FEED_SETTLE_S`, nor
   `INGEST_BUFFERED_ERASURE_DELETE` or its `{"bufferedErasureDelete":true}` schedule (lambda.ts:755-768 handles
   them). The runbook forbids arming before the erasure lane exists (runbooks/sessions-prod-session-pipeline.md, "Buffered search feed").
3. **045010 applied** (window 2). With SECOND_VERSION on and 045010 missing, every stage is refused
   (`sql_same_second_unavailable`).
4. **External readers.** relayhistory-cloud main (a7c0ea50) CH readers for `GET /v1/sessions?q=`, `/v1/events?q=`
   and the Neon tail reconcile do not filter `origin = 'canonical'`. The relayhistory-cloud owner confirms the fix
   is deployed, and Khaliq confirms in his own words.
5. §6 reader gate on and verified; `BUFFERED_SEARCH_FEED_GATE_DECLARED="buffered-search-gate/v1"` (buffered-search-feed.ts:254-262).

### 7.2 Role, secret, lanes (each its own OK)
- Feed role LOGIN: `supabase/bootstrap/buffered_search_feed_runtime_role.sql` as postgres, password from 1Password
  in memory; verify file before and after (feed packet §2.4). As late as possible.
- Pipeline secret fields `buffered_search_feed_database_url` / `_ca_pem` (runbook secret procedure; force a warm
  Lambda refresh).
- Erasure lane and SECOND_VERSION on, per the stack change that 7.1(2) needs.

### 7.3 Enable with the #448 knobs
#448 (merged b931546d) exposes three knobs under `transcriptStore.sessionPipeline.bufferedSearchFeed`
(stacks/aws/session-pipeline.ts:206-211 bounds; runtime defaults buffered-search-feed.ts:290-296):

| knob | env | default | bounds |
|---|---|---|---|
| `pageSize` | INGEST_BUFFERED_SEARCH_FEED_PAGE | 50 | 1-200 |
| `stagesPerSecond` | INGEST_BUFFERED_SEARCH_FEED_RATE | 5 | 1-50 |
| `budgetMs` | INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS | 50000 | 5000-240000 |
| (not exposed) | INGEST_BUFFERED_SEARCH_FEED_SETTLE_S | 300 | 180-3600 |

Input: `bufferedSearchFeed = {enabled: true, gateDeclared: "buffered-search-gate/v1", pageSize: 50, stagesPerSecond: 5, budgetMs: 50000}`
(start at defaults; raise `stagesPerSecond` only after a clean hour). The stack refuses `enabled` without the exact
`gateDeclared` or without `searchIndex: true` (:199-204); the rule is ENABLED only when `enabled` (:486-492).

### 7.4 Backfill sizing (feed packet §5.1 arithmetic)
The backfill is the feeder's first runs (cursor `backfill-20261105`, since 2026-11-05T05:33:45Z). One invocation
per minute; per-invocation ceiling = rate × budget seconds. Best case (per-stage latency unmeasured):
- Defaults: 5 × 50 = **250 stages/min = 15,000 stages/h**. 167,000 stages ÷ 250 = 668 min ≈ **11.1 h**.
  As CH rows (R rows per stage, unmeasured): R = 10 → 16,700 stages ≈ 67 min; R = 100 → 1,670 stages ≈ 7 min.
- Rate 20, budget 50 s: 1,000 stages/min; 167,000 ÷ 1,000 = 167 min ≈ 2.8 h.
- Rate 50, budget 50 s: 2,500 stages/min; 167,000 ÷ 2,500 ≈ 67 min.
- Keep `budgetMs` ≤ 50000: the schedule is 1 minute, and a budget above 60 s may overlap the next invocation
  (UNVERIFIED; the Lambda timeout is 300 s). Scale with the rate, not the budget.
- Global hours = N_stages ÷ (rate × budget s × 60). Counts-only pre-read, quiet period:
```sql
SELECT count(*) FROM sessions.ingest_stage_intents
 WHERE created_at >= '2026-11-05T05:33:45Z' AND receipt_state = 'acknowledged' AND cleanup_state = 'live';
```
- Window-start skip (feed G7): with the 14-day CH window, cutover-day events start being skipped around
  **2026-11-19**; the backfill must finish before then.
Stop rules (feed §5.2): parts or merges above baseline, any open mutation, any `ok:false` run, read API 5xx or
`search_timeout` rise, PG IO stop rules (p95 claim latency, 55P03/57014, IO waits). Stop = disable the rule.

## 8. Canary and rollback per step (counts only)

| step | canary (pass) | rollback |
|---|---|---|
| 2.1 role bootstrap | pre-check row present, NOLOGIN, not elevated | leave the bare role (inert); dropping it is an operator decision |
| 2.2 boundary | prosrc sha = a7a9edfe…0125; runtime role literal unchanged | none needed (superset); rerunning the 3d0b4191 file would block window 2 |
| 3 CH DDL | 2 rows in system.columns; only `canonical` origin | leave columns |
| 4.1 Worker {G,H} | ingest 2xx/503 counts at baseline | redeploy previous inputs (expected = G) |
| 4.2 overlap open | window record with from = G_CUR, to = H | `ingest-generation-overlap.ts close --from "$G_CUR" --to "$H"` |
| 4.3 migrator | "migration applied" x8, marker (H, pass) | failed step rolls back; marker stays (H, pending); fix the cause and rerun from the pin. Restore G PASS only with the canonical migrator from the G pin (plan A §8). Never mark_pass. |
| 4.4-4.5 rebind | 8 ledger rows; rebind counts; no open window | none after PASS; fix forward. Down files only on the DB actor's word, with Khaliq. |
| 4.6 G2 bulk | `"status":"complete"`, zero failures | rerun to resume |
| 4.7 runtimes/cleanup | no `schema_pending`/`validation_pending`; 503 at baseline | redeploy the previous image only before cleanup |
| 5 writer | claim p95 within 20%; side rows growing | `write_enabled = false` (clears cursor and stamp) |
| 5 backfill | `completionMarked:true`; pairs equal; stale 0 | cancel; rerun resumes |
| 5 reader | list results unchanged (list-fix §4 canary) | `read_enabled = false` |
| 6 reader gate | 5xx/`search_timeout` counts at baseline | redeploy without the input (feeder off first) |
| 7 feeder | run log `ok:true`; listed/inserted/skipped counts; cursor counts rising | disable the rule; durable stop `enabled: false` |

Forward fix is the norm. The per-migration down files are not run on a real database except on the DB actor's
word, with Khaliq.

## 9. Order-of-operations checklist

Gate owner = Khaliq (his own words, per step). Actor = Executor (brian-db-takeover-linuyc).

1. [ ] Verify pin, digests, H; read G_CUR from the live marker (= plan-A G_new). Read-only. Gate: Khaliq (scope). Actor: Executor.
2. [ ] Baselines: ingest 2xx/503, claim p95, 55P03/57014, read API 5xx, CH parts/merges. Read-only. Gate: Khaliq. Actor: Executor.
3. [ ] §2.1 role pre-checks; run session_read_api_roles.sql only if `bulls96_digest_narrative` is missing. Gate: Khaliq. Actor: Executor.
4. [ ] §2.2 boundary re-bootstrap from ce9b4464, prosrc sha check. Gate: Khaliq. Actor: Executor.
5. [ ] §3 CH DDL and system.columns check (any time before step 15). Gate: Khaliq. Actor: Executor.
6. [ ] §4.1 Worker deploy expected = G_CUR, overlap = H. Gate: Khaliq. Actor: Executor.
7. [ ] §4.2 overlap open (TTL 3600, no admit-during-apply). Gate: Khaliq. Actor: Executor.
8. [ ] §4.3 migrator from ce9b4464 (8 steps). Gate: Khaliq. Actor: Executor.
9. [ ] §4.4 verify ledger (8 rows), PASS for H, list-fix flags off. Read-only. Gate: Khaliq. Actor: Executor.
10. [ ] §4.5 rebind dry run, then `--execute`; overlap status closed. Gate: Khaliq. Actor: Executor.
11. [ ] §4.6 G2 bulk re-proof for H. Gate: Khaliq. Actor: Executor.
12. [ ] §4.7 rebuild schema-checking runtimes from the pin; Worker cleanup expected = H. Gate: Khaliq. Actor: Executor.
13. [ ] Window-2 canary (§8 rows 4.3-4.7). Gate: Khaliq. Actor: Executor.
14. [ ] §5 writer enable → dry run → backfill chunks → sentinel → reader enable (each its own OK). Gate: Khaliq. Actor: Executor.
15. [ ] §6 reader deploy with `buffered_search_gate=on`; canary. Gate: Khaliq. Actor: Executor.
16. [ ] **STOP** until §7.1 holds: ceiling census read and pinned on main; stack pass-through for SECOND_VERSION and the erasure lane landed; relayhistory-cloud fix deployed (owner confirms, Khaliq confirms). Gate: Khaliq. Actor: Executor (census read), code owners (commits).
17. [ ] §7.2 feed role LOGIN and pipeline secret. Gate: Khaliq. Actor: Executor.
18. [ ] §7.2 erasure lane and SECOND_VERSION on. Gate: Khaliq. Actor: Executor.
19. [ ] §7.3 feeder enable with knobs (defaults first); §7.4 pre-read for sizing. Gate: Khaliq. Actor: Executor.
20. [ ] Watch per §7.4 stop rules until the backfill cursor catches up (before 2026-11-19); report counts only. Gate: Khaliq. Actor: Executor.
