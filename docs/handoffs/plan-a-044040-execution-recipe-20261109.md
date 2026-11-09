<!-- Operator execution recipe for Khaliq's plan A (one paused migrator run 044020+044030+044040 with
     HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true) and the prefix-first variant (044020+044030 at b254b615, GIN later).
     Written 2026-11-09 by layered-sessions-pipeline for Brian (sole production actor) at pipeline-lead's request.
     Source-only: nothing here was run against any database. Every write, deploy, dispatch and flag flip in this
     document needs Khaliq's explicit word. Production identifiers are placeholders. Durations are untested estimates.
     Items marked UNVERIFIED were not confirmed in code and must be checked by the operator before use. -->
# Operator recipe: plan A, one paused migrator run 044020 + 044030 + 044040 (96Bulls)

Run order (summary):
1. Verify the source pin 3d0b4191 in a clean scratch clone, compute G_new, and capture G_cur, the ledger tail (044010) and the marker. Read-only.
2. Khaliq gives his word; Brian deploys the ingest Worker with expectedGeneration=G_cur and overlapGeneration=G_new (zero-503 §3.1). Brian also opens the owner direct connection from 1Password.
3. Brian runs ONE CLI migrator invocation from the pin with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true. The pending marker is the de facto ingest pause (generation fence, 55000/503).
4. Verify indexes, ledger, RPC and PASS for G_new, then run the rebind G_cur->G_new (dry run, then --execute). Uploads resume here.
5. Run the G2 bulk parity re-proof, a canary by natural traffic, and a later Worker deploy with expectedGeneration=G_new. Roll forward; no down-migrations on real databases.

Authority: every write, deploy and dispatch below needs Khaliq's explicit word, and Brian's hands carry it out (AGENTS.md "Deploys ... migrations to any real database still need Khaliq's separate, explicit OK"; operator-packet.md:3-4).
Citations are `path:line` at pin 3d0b4191. The cited files are byte-identical at main 14523d33, except src/hoopdb/migrate-guards.ts, which has a 1-line change.

## Plan A conflicts with the runbook. Khaliq must acknowledge this before the run
- runbooks/session-directory-search-worker-bootstrap.md:185 says HOOPDB_ALLOW_BLOCKING_INDEX_BUILD: **"Never set it on production"**. Both the release workflow profile and the owner apply profile (`ownerProfile`, src/hoopdb/owner-operator-apply.ts:100-110) cannot opt in. Only the CLI `bun src/hoopdb/migrate.ts` reads it from the environment.
- docs/handoffs/buffered-stages-session-first-044040/operator-packet.md:159-164 (B0) says the blocking build is "not for production".
- Plan A therefore uses the CLI route, not `scripts/schema-release-owner-apply.ts`. It is a deliberate, documented divergence, and Khaliq's approval quote should name it.

## 1. Source pin and verification; how to invoke
Pin: `3d0b419110f858043962a8a6a18f46cdc7efe154` (merge of #435). The next migration-adding commit on main is 7513d852 (044050), followed by #434, #439 and #441. The migrator has no target-slot option (docs/design/zero-503-schema-windows.md:219). A run from main would therefore also apply 044050 and later, so it **must run from a checkout at the pin**.
```sh
git clone -q --reference /home/agent/build/sessions-pipeline \
  https://github.com/AgentWorkforce/relayhistory-pipeline.git <scratch>/recipe-044040
cd <scratch>/recipe-044040 && git checkout -q 3d0b419110f858043962a8a6a18f46cdc7efe154
git merge-base --is-ancestor 3d0b4191 origin/main && \
  git rev-list --first-parent origin/main | grep -q '^3d0b4191' && echo first-parent-ok
git ls-tree --name-only 3d0b4191 supabase/migrations/ | tail -1
#   expect: supabase/migrations/20261105044040_buffered_stages_session_first.sql
git diff --stat 3d0b4191 origin/main -- supabase/migrations/20261105044020_claim_bootstrap_org_indexes.sql \
  supabase/migrations/20261105044030_ingest_generation_overlap.sql \
  supabase/migrations/20261105044040_buffered_stages_session_first.sql      # expect: empty
sha256sum supabase/migrations/202611050440{2,3,4}0_*.sql
#   044020 35787e1db39a74dbc0d3520efac2f1bb920d8e559edc78292649cfddb09a7a25
#   044030 cd18205603e6ad192654ecac57619b7a3a99c2b7be008cbdcb4399f6fa8c9d47
#   044040 a3e0cb8f98ba9f280b8306e830146a108db59df5cace893d579260dbcd95e28e
git status --porcelain     # expect: empty (the parity operator refuses a dirty tree, §6)
bun install --frozen-lockfile
```
I verified the first-parent check, the tail, the empty diff and the three sha256 values against the local repo.

Ways to invoke the migrator:
- Image: docker/entrypoint.sh:6 runs `bun run /app/src/hoopdb/migrate.ts` when `SERVICE=hoopdb-migrate`. migrate.ts:1-9 describes it as a one-shot container. The image tag convention for a pin is UNVERIFIED. package.json has no `migrate` script.
- CLI (plan A): `bun src/hoopdb/migrate.ts` from the pinned checkout. This is the only route that honours the blocking opt-in (bootstrap runbook :185).
- Other CLI modes: `--generation` prints the generation (migrate.ts:1232) and `--activation-check` (:1164). Plain `migrate` is the default. `DEBUG=1` prints the raw error.

## 2. Migrator command, environment, connection and role
The DSN comes from **HOOPDB_URL**. src/shared/db.ts:40-41 `openHoopDb(url = required("HOOPDB_URL"))` passes it unchanged as `connectionString` to `pg.Pool`. migrate() calls openHoopDb, then `pool.connect()` (migrate.ts ~:1118).
Environment (literal ownerProfile values, owner-operator-apply.ts:100-110, plus the opt-in):
```sh
export HOOPDB_DB_MODE=supabase                    # migrate.ts:161
export HOOPDB_SUPABASE_SHIM=false                 # :173
export HOOPDB_ROLE_BOOTSTRAP=false                # :174
export HOOPDB_ROLE_VERIFY=true                    # :175 (default true)
export HOOPDB_RUNTIME_ROLE=bulls96_runtime        # :210
export HOOPDB_RUNTIME_ROLE_LOGIN=true             # :216
export HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke   # :241 (old ..._APPROVED is refused, :236)
export HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true     # doc :132-135, parsed :257 strictFlag
# Do NOT set HOOPDB_RUNTIME_WORKLOAD_ROLE (:222) unless the production profile already does (UNVERIFIED).
```
strictFlag (:141-153) accepts only `1|true|0|false`; any other value fails with db_profile_invalid. Every migration and verify transaction runs `set_config('hoopdb.allow_blocking_index_build', 'on'|'off', true)` (:643). The binding is transaction-local but applies to the whole run. Two migrations read it: 044020 (`v_allow` :47, refusal :79-83) and 044040 (`v_allow` :63, refusal ~:78-96).

Connection: the **owner direct connection from 1Password** (not the pooler, not Hyperdrive). Supavisor drops startup `options` (owner-operator-apply.ts ~:293).
```sh
HOOPDB_URL='<owner-direct-url>?options=-c%20statement_timeout%3D120min%20-c%20maintenance_work_mem%3D2GB%20-c%20max_parallel_maintenance_workers%3D2' \
  bun src/hoopdb/migrate.ts 2>&1 | tee <scratch>/migrate-044040.log
```
- Why the options are needed: the migrator never sets statement_timeout. A grep of migrate.ts finds only marker strings at ~:530. The run inherits the session default otherwise.
- How the options reach the server: pg 8.23.1 (package.json:60). pg-connection-string copies every query parameter into the config (index.js:33-34). connection-parameters.js:83 reads `options`, and the `val()` helper (:9-15) falls back to `PGOPTIONS`. client.js:567-568 sends it in the startup message. An equivalent alternative is `PGOPTIONS='-c statement_timeout=120min -c maintenance_work_mem=2GB -c max_parallel_maintenance_workers=2'`.
- No short lock_timeout. 044020:84 and 044040:86 take `LOCK TABLE ... IN SHARE MODE` themselves. 044030:55 sets its own `SET LOCAL lock_timeout='5s'`.
- Role: 044030:57-75 requires current_user to own the sessions tables and functions. Log in as the owner or append `-c role=<owner-role>` to options. Which of the two applies in production is UNVERIFIED; the owner route uses `SET SESSION ROLE postgres` (owner-operator-apply.ts:281).
- Before the run, confirm on a second session: `SHOW statement_timeout; SHOW maintenance_work_mem;` (or check them in pg_stat_activity for application_name of the migrator).

Behaviour of the run:
- It takes `pg_advisory_lock(961996, 1)` (:564) and writes the pending marker before any step (~:597-598, logs "role validation pending").
- It applies **every pending step in one invocation**, each in its own BEGIN (:724), ledger INSERT (:729) and COMMIT (:732). A failure rolls back that step (:734) and stops the run.
- Verify steps then flip the marker to pass (~:669-710; 40P01 retried 3x).
- Unlock happens at :763.

## 3. Pausing ingest writes, and the reverse
The ingest Worker has **no maintenance or pause flag**. A grep of src/session-ingest for MAINTENANCE/PAUSE found none; worker.ts has only INGEST_EXPECT_GENERATION and INGEST_ACCEPT_GENERATIONS (:31-34, :105-107).
- The supported mechanism is the **generation fence**. From the pending marker write (migrate.ts ~:597) until the rebind commits, buffered uploads get 55000, which the Worker surfaces as 503 (zero-503 doc :27-31, :49-59, §3.1 :213-234). That is the de facto pause, and nobody flips it separately.
- Pre-window (optional, §3.1): Brian deploys the Worker with expectedGeneration=G_cur and overlapGeneration=G_new (stacks/cloudflare/session-ingest.ts:233-238, :296-297). The 044030 overlap CLI (scripts/ingest-generation-overlap.ts, doc :241) **cannot** open a window in this run, because 044030 creates it. Expect the full outage from pending marker to rebind.
- Reverse: the rebind in §5. The marker must be PASS for G_new first (src/hoopdb/ingest-generation-rebind.ts:32-34, refusal `readiness_not_pass_for_target`).
- Consumers and readers: schemaVerdict (src/shared/db.ts:197-260) returns schema_pending or validation_pending, and waitForSchema (:262) waits. Each consumer's exact behaviour during the window is UNVERIFIED. Record a pre-window 503 baseline separately from intentional refusals (generation-window.md :51).

## 4. Expected steps, outputs, ledger, verification and monitoring
Steps applied: `upstream:20261105044020_...`, `upstream:20261105044030_...`, `upstream:20261105044040_...` (schemaSteps ~:296-307), then the verify steps.
Expected log (JSON lines): "role validation pending", then "migration applied" {reason: step name} x3, "role validation passed", and "hoopdb migrations complete" {count, skipped}.
- The migrations raise no NOTICE, so adopt x7 and build x1 are not logged individually. 044020 specs :50-66 and checks :104-112 (42P07 or 55000 on a mismatch).
- On a failure the log shows "migration failed" {reason, error_code} (:734).
Ledger: `sessions.__96bulls_migrations(name text PK, sha256 text, applied_at timestamptz DEFAULT now())` (~:79, :578-582).
Monitor from a second session (claim-bootstrap packet :234-325 recommends a second session for watching and cancelling):
```sql
SELECT pid, state, wait_event_type, wait_event, now()-query_start AS age, left(query,80)
  FROM pg_stat_activity WHERE application_name LIKE '%hoopdb%' OR wait_event_type='Lock';
SELECT pid, phase, blocks_done, blocks_total, tuples_done, tuples_total
  FROM pg_stat_progress_create_index;                      -- operator-packet.md:77
```
Rough durations (UNTESTED, 4 vCPU / 16 GB): 044020 btree on 21.3M rows / 24.6 GB takes 10-30 min, 044030 seconds, and the 044040 GIN on 12.7 GB 15-45 min. The SHARE lock blocks writes to each table until that step's COMMIT.
Verification after the run:
```sql
SELECT name, sha256, applied_at FROM sessions.__96bulls_migrations
 WHERE name LIKE 'upstream:20261105044%' ORDER BY name;      -- expect 044020/044030/044040, sha as §1
SELECT c.relname, i.indisvalid, i.indisready FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
 WHERE c.relname IN ('ingest_lazy_native_slots_org_id_idx','ingest_claim_attempts_records_gin_idx');
SELECT generation, status FROM <role-validation-table> WHERE name = <marker-name>;  -- expect G_new, pass
```
Use operator-packet.md:86-88 for index validity, :134-147 for EXPLAIN, and :189-190 for B4 (prosrc count = 2 and the 044040 ledger row). The marker table and name constants are in src/hoopdb (ROLE_VALIDATION_TABLE/NAME); the literal names are UNVERIFIED here.

## 5. Rebind, readiness PASS and Worker generation; deploy workflow
G_new is computed from the pinned checkout. Whether `--generation` needs a DB connection is UNVERIFIED.
```sh
G_NEW=$(bun src/hoopdb/migrate.ts --generation)    # migrate.ts:1232; bootstrap runbook :154-160
```
The migrator itself writes PASS for G_new (verify loop ~:669-710). **Never mark PASS manually** (guarded-session-schema-release.md). The external mark_pass (bootstrap runbook :154-160) is not part of plan A.
Rebind (scripts/ingest-generation-rebind.ts:3-4; it uses the CLI temp login, `SET SESSION ROLE postgres`, SUPABASE_PROJECT_REF :18, and statement_timeout 30s / lock_timeout 5s :21):
```sh
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-rebind.ts --from "$G_CUR" --to "$G_NEW"
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-rebind.ts --from "$G_CUR" --to "$G_NEW" --execute
```
The rebind prints JSON counts only (:26), or `{"status":"refused","code":...}` with exit code 1 (:27). It is idempotent.
- Worker variable: INGEST_EXPECT_GENERATION, plus optional INGEST_ACCEPT_GENERATIONS. They come from the nonsecret `sessionIngest.expectedGeneration` / `overlapGeneration` inputs (session-ingest.ts:222-238, :296-297; alchemy.cloudflare.run.ts:65-69).
- Deploy path: `scripts/private-ingest-operator.ts plan|deploy --expected-sha <sha> --inputs-sha256 <h> [--provenance-sha256 <h>]` (generation-window.md:136-147; operator modes at private-ingest-operator.ts:87, :447). The flag semantics are taken from the handoff doc and were not re-read in the script, so they are UNVERIFIED.
- .github/workflows/agent-deploy.yml is the host Agent Deploy, not the ingest Worker. Workers are "separate workflow_dispatch workflows that Khaliq gates" (:14).
- Final step: a later deploy with expectedGeneration=G_new and no overlap (zero-503 §3.1).
- **Deploys and dispatches: Khaliq's word, Brian's hands.**

### 5a. Build constraint for plan A (which source each runtime must be built from)
- The ingest Worker does not compute schema readiness; its generation is configuration (INGEST_EXPECT_GENERATION / INGEST_ACCEPT_GENERATIONS from `sessionIngest.expectedGeneration` / `overlapGeneration`, stacks/cloudflare/session-ingest.ts:124-130, :232; src/session-ingest/worker.ts:105-107). Its code for the window must be 3d0b4191 or later (it carries the two-generation offer). The G_new it is given must be computed from the 3d0b4191 checkout (`bun src/hoopdb/migrate.ts --generation`).
- Every runtime that computes `expectedSchema()` at start (search worker src/shared/worker.ts:354-355; alleyoop ingest; indexer; consumers src/session-pipeline/consumer-service.ts:1127; buffered GC and publication via derivedSchemaAssets) must be **built from 3d0b4191** for the window: schemaVerdict (src/shared/db.ts:218-236) reports `schema_pending` whenever one of the build's migration names is missing from the ledger, so an image built from main (044050 and later present) waits forever against a 044040 ledger, and a pre-window image expects G_cur and reports `validation_pending` once PASS moves to G_new (db.ts:237-243).
- The rebind (section 5) and the G2 bulk parity proof (section 6, `--expect-source-sha 3d0b419110f858043962a8a6a18f46cdc7efe154`) run from the same clean 3d0b4191 checkout.
- Moving those runtimes to a later main (044050+) is a separate later window, with its own pending→PASS cycle and rebind, now protected by the 044030 overlap window (section 9c caveats apply).

## 6. G2 bulk re-proof (owner-buffered-parity-operator)
Header and usage are at scripts/owner-buffered-parity-operator.ts:1-40, with allowed flags at :115.
- Credentials arrive as one JSON object `{neonUrl, supabaseUrl}` on an approved RAM-only pipe. They are never passed in argv, environment or a file.
- The checkout HEAD must equal `--expect-source-sha` and the tree must be clean (verifyCheckout :184-195, `checkout_pin_mismatch`).
```sh
<approved RAM-only credential pipe> | bun run scripts/owner-buffered-parity-operator.ts bulk \
  --expect-source-sha <checked-out-sha> --expect-target-generation "$G_NEW" \
  --batch-size 25 --confirm-write owner-buffered-parity-bulk/v1
```
- PASS output is one JSON receipt with `"status":"complete"`, mode bulk, sourceSha, targetGeneration and counts only, with exit code 0 (:457-458). Any other status exits 3; a refusal exits 1 (:462).
- A stopped run is resumed by rerunning the same command (header :25-29).
- Never relabel target_generation (operator-packet.md:179-187).
- Whether the expected source sha should be the pin or main is UNVERIFIED. Check out the sha that the parity approval names.

## 7. Canary, then unpause
- No ingest-specific canary script exists. scripts/proof-publication-canary.ts and docs/runbooks/canonical-publication-canary.md cover the *publication* path and authorize no deployment (:3-5), so they are not used here.
- The documented post-window check (generation-window.md:225) is one real upload with ACK in an existing scope and a canonical readback where qualified. Use sanitized natural-traffic diagnostics, report counts only, and invent no synthetic production ACK.
- The unpause is the §5 rebind `--execute`. Plan A has no other switch.
- Verify afterwards:
  - ingest 2xx returns to the pre-window level;
  - no generation_not_ready or readiness errors;
  - 503s are compared against the pre-window baseline, not against zero;
  - the marker is PASS for G_new and no schema is pending.
- Report the outage interval from the pending-marker time to the rebind commit (operator-packet.md:166-177).

## 8. Rollback at each step
**Rule: no down-migrations on real databases.** Rollback files exist in supabase/rollbacks/ (044020/044030/044040). A re-apply after one would need the ledger row removed under a separate OK (operator-packet.md:192-194). The guarded runbook forbids erasing migration history or manual PASS. **A failed migration transaction leaves its ledger row unwritten.** Earlier steps stay committed and the marker stays pending, so uploads keep getting 503 (guarded-session-schema-release.md:36).
- Before the run: nothing is applied. Kill the session to release the lock if needed, or redeploy the previous Worker inputs.
- 044020 fails (for example 57014 statement_timeout during the blocking build, or a lock wait): the step rolls back, so no index is adopted, no ledger row is written and no partial index remains. Fix the cause and rerun the same command, or fall back to a CIC prebuild (a separate plan that needs Khaliq). The marker stays pending until a successful run.
- After 044020 only (044030 failed): the indexes are additive, so leave them. A rerun resumes at 044030.
- After 044030: it is additive. Overlap tables are added, and ingest_require_generation and ingest_lazy_guard are replaced (:294, :312). With no window open the behaviour is as strict as before (:43). The down file (:53-54) exists, but do not run it; fix forward.
- After 044040: the GIN index is additive and the RPC bodies are replaced (:119, :345). Fix forward. The down file needs a separate OK.
- After PASS for G_new: redeploying G_cur code or rebinding to G_cur **cannot** restore G_cur PASS (generation-window.md:235). Fix forward to G_new.
- Worker rollback: redeploy the previous reviewed sessionIngest inputs (expectedGeneration=G_cur). This only makes sense before the rebind executes. It needs Khaliq's word and Brian's hands.
- A rebind refusal rolls back its own transaction (ingest-generation-rebind.ts:34, generation-window.md:235).

## 9. Appendix: prefix-first variant (NOT chosen; Khaliq confirmed plan A in pipeline-lead's session, 2026-11-09) (044020+044030 now at pin b254b615, 044040 in a later window)
Pin: `b254b61511d8c52af5c7c3163a49b38629d935a3` (merge of #433). I verified that it is a first-parent ancestor of main, that `git ls-tree` of its migrations ends at `20261105044030_ingest_generation_overlap.sql`, and that `git diff b254b615 main` on the 044020 and 044030 files is empty.
- Out of band and before the window: prebuild the index CONCURRENTLY, using the exact definition at 044020:65-66 with CONCURRENTLY added. 044020 then adopts it after its definition and validity checks (:104-112).
- **Do not set** HOOPDB_ALLOW_BLOCKING_INDEX_BUILD. 044020 refuses with 55000 if the index is still absent (:79-83).
- No blocking opt-in is needed, so the supported owner-apply route (scripts/schema-release-owner-apply.ts, 5 min statement / 5 s lock timeout, :147-148) becomes usable. Whether its pinned ledger digest (owner-operator-apply.ts:21-36) still matches production is UNVERIFIED. Otherwise use the §2 CLI from this pin, without the opt-in and with a short statement_timeout.
```sql
-- owner direct connection from 1Password, autocommit, NOT inside the migrator:
SET statement_timeout = '120min'; SET maintenance_work_mem = '2GB'; SET max_parallel_maintenance_workers = 2;
CREATE INDEX CONCURRENTLY ingest_lazy_native_slots_org_id_idx ON sessions.ingest_lazy_native_slots USING btree (org_id);
-- then confirm indisvalid = true (operator-packet.md:86-88 pattern); an INVALID leftover must be dropped (separate OK) before retry
```
Memory: "CONCURRENTLY index = no slot, migrator is transactional", so this prebuild is not a ledger row.

**(a) Which source the Worker and readiness builds must come from**
- Readiness (schemaVerdict, src/shared/db.ts:197-260) compares against `expectedSchema()` (src/hoopdb/migrate.ts:384-397 at b254b615).
  - `names` is every schema step found in the image's own `supabase/migrations` and `src/hoopdb/migrations` (schemaSteps).
  - `generation` is `generation()` (:353-382): a sha256 over each step's name and file sha256, plus the seven verifier files.
  - db.ts:228 returns `schema_pending` if any expected name is missing from the ledger. db.ts:232 requires marker `status='pass'` and `generation === expected.generation`.
- So every image that computes expectedSchema at runtime must be **built from b254b615** to pass at 044030. A main or 3d0b4191 image expects 044040 or later and reports `schema_pending`. A pre-window image expects G_cur and reports `validation_pending` after PASS for G1.
- The images that compute it are:
  - the search worker (src/shared/worker.ts:354-355);
  - alleyoop ingest (src/alleyoop/ingest.ts:265);
  - the indexer (src/hoopdb/indexer.ts:21);
  - the consumers (src/session-pipeline/consumer-service.ts:1127, consumer-bootstrap.ts:153);
  - the buffered GC and publication. These read derivedSchemaAssets (derived-schema.ts:13) and refuse with `compiled_generation_mismatch` unless the compiled generation equals their configured generation (buffered-gc-job.ts:76; publication-runtime.ts:60).
- The ingest Worker does not call schemaVerdict. Its generation is **config**, not compiled: `sessionIngest.expectedGeneration` / `overlapGeneration` becomes INGEST_EXPECT_GENERATION and INGEST_ACCEPT_GENERATIONS (stacks/cloudflare/session-ingest.ts:222-238, :296-297).
- Its code should still be b254b615 or later, because that code carries the two-generation offer (worker.ts INGEST_ACCEPT_GENERATIONS; zero-503 §2).
- Set G1 from the pinned checkout:
```sh
git checkout -q b254b61511d8c52af5c7c3163a49b38629d935a3 && git status --porcelain   # expect empty
G1=$(bun src/hoopdb/migrate.ts --generation)    # whether this needs a DB connection: UNVERIFIED
```
Lambda and other runtime variables that carry the generation (for example INGEST_PUBLISH_EXPECT_GENERATION, generation-window.md:133) were not traced per runtime. UNVERIFIED.

**(b) Window 1: apply, rebind and G2 proof** (Khaliq's word, Brian's hands)
```sh
# Worker first (zero-503 §3.1 step 2): expectedGeneration=G_cur, overlapGeneration=G1, code at b254b615
# apply from b254b615 (no HOOPDB_ALLOW_BLOCKING_INDEX_BUILD); expect "migration applied" x2, then PASS for G1
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-rebind.ts --from "$G_CUR" --to "$G1"
SUPABASE_PROJECT_REF=<project-ref> bun scripts/ingest-generation-rebind.ts --from "$G_CUR" --to "$G1" --execute
<approved RAM-only credential pipe> | bun run scripts/owner-buffered-parity-operator.ts bulk \
  --expect-source-sha b254b61511d8c52af5c7c3163a49b38629d935a3 --expect-target-generation "$G1" \
  --batch-size 25 --confirm-write owner-buffered-parity-bulk/v1
```
- The parity operator exists at b254b615 with the same bulk confirmation (:70) and checkout check (:185, :429). It must run from a clean checkout whose HEAD is b254b615. PASS is `"status":"complete"` with exit code 0, as in §6.
- This window is still the one accepted outage (pending marker to rebind). The overlap table does not exist until 044030 commits in this same run (zero-503 :213-234; 044030 header :50-51).

**(c) Does 044030 make the later 044040 window zero-503?** Partly, and only under conditions.
- Once 044030 is applied, a later G1 to G2 window can be overlap-protected (zero-503 §3.2, :236-260):
  1. Deploy the Worker with expected=G1 and overlap=G2.
  2. Run `bun scripts/ingest-generation-overlap.ts open --from G1 --to G2 --ttl-seconds <60..14400> --approval-sha <sha>` immediately before the apply (script header :1-12). It requires marker PASS for `from` (044030:18-21).
  3. Apply. Uploads continue at G1.
  4. Rebind G1 to G2. The rebind closes the window in its own transaction (src/hoopdb/ingest-generation-rebind.ts:13-16, :35-42, :69-71; `overlap_window_mismatch` refusal).
  5. Clean up the Worker (expected=G2).
- **Caveat 1, admitDuringApply.** It is off by default (zero-503 :242; 044030:31-33). With it off, the window admits G1 only while the marker is `pass`. Uploads are therefore still refused from pending until PASS, which means for the whole 044040 apply. They then resume at G1 until the rebind. The outage shrinks from "pending to rebind" to "pending to PASS"; it does not go to zero.
- **Caveat 2, zero refusals.** Reaching zero needs `--admit-during-apply`, which requires a full-union rehearsal under concurrent uploads first (§4, :262-280). Even then, it covers only buffered upload entry points, never publication (044030:33-35).
- **Caveat 3, blocking GIN build.** Lock waits are not fence refusals (zero-503 :271-278). A blocking GIN build in 044040 holds SHARE on the claim-attempts table and can time out writers (55P03/57014) regardless of the window. A zero-503 044040 therefore also wants the GIN prebuilt CONCURRENTLY and adopted, not built blocking.
- 044040 keeps its RPC signatures (header :20), so a G1 Worker stays contract-compatible. The rehearsal proof is still required (zero-503 :262-266).

**(d) Two-deploy cost**
- Window 1 needs a Worker deploy at b254b615 (expected=G_cur, overlap=G1) before the apply, and optionally a cleanup deploy (expected=G1).
- Window 2 needs a Worker deploy at the 044040 pin (expected=G1, overlap=G2) before the apply, then a cleanup deploy (expected=G2).
- Every compiled-generation runtime (search worker, GC, consumers, indexer, publisher if active) must be rebuilt **twice**, at b254b615 and at the 044040 pin.
- Two G2-style bulk parity proofs are needed, one per target generation.
- Two approvals, two rebinds and two outage reports are needed.
- Plan A needs one of each.
- What the extra cost buys: no blocking build on production, and a window-1 outage bounded by the short adopt-only apply instead of a 21.3M-row blocking build.
