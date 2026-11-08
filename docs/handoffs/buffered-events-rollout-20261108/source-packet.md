# Buffered events (#338 / #339) rollout source packet: prerequisites only

Source: `main` at c858e7fe (merge of #339; #338 merged as 9de35b72). Every `path:Lx` below is a
line in that tree. This packet is documentation. It activates nothing, recommends no activation,
and is not an approval. Production identifiers are written as "redacted". No credential value
appears here. Tags: `[source-ready]` = present and default-off on main;
`[operator: needs Khaliq OK]` = an activation step that only the operator lane (pipeline-lead)
executes, after Khaliq's own OK in his own words; `[missing]` = not in source on main.

## 1. Scope and status

- #338 adds a per-hit privacy gate for buffered-origin search hits in the read Worker:
  `src/session-read-api/buffered-search-gate.ts` (rules L1-70) and the SECURITY DEFINER reader
  `sessions.buffered_search_gate_for` (`supabase/migrations/20261105043860_buffered_search_gate.sql:180-370`).
- #339 adds the feeder that projects buffered stages into ClickHouse `transcript_events` with
  `origin='buffered'` (`src/session-pipeline/buffered-search-feed.ts:1-53`, `:77-80`), plus a cursor table and
  three RPCs (`supabase/migrations/20261105043870_buffered_search_feed_cursor.sql:107-254`).
- Both are default-off.
  - Gate: only the literal `on` enables it (`buffered-search-gate.ts:95-98`). Absent means canonical rows only
    (`src/session-read-api/app.ts:165-169`). The Lambda read API never wires the gate (same lines).
  - Feeder: `INGEST_BUFFERED_SEARCH_FEED` is `off` by default, and any value other than `off`/`on` refuses
    (`buffered-search-feed.ts:127-131`). The Lambda returns before any secret or connection when the flag is not
    `on` (`src/session-pipeline/lambda.ts:654-656`).
- Status: the deployed image is 8bb18be5, which includes neither PR (trusted program fact). No stack sets
  `INGEST_BUFFERED_SEARCH_FEED`, `BUFFERED_SEARCH_FEED_GATE_DECLARED` or the feeder secret fields. A grep of
  `stacks/` and `.github/` at c858e7fe finds none, and `lambda.ts:96-100` says "no deploy wiring sets it".
- Ordering is a source invariant:
  - #338 must be active and verified BEFORE #339 is enabled (`.github/workflows/deploy-session-read-worker.yml:37-39`;
    `runbooks/sessions-prod-session-read-worker.md:78`).
  - The feeder refuses to run unless `BUFFERED_SEARCH_FEED_GATE_DECLARED` equals `buffered-search-gate/v1`
    (`buffered-search-feed.ts:113-121`; contract id at `buffered-search-gate.ts:90`; checked again at `lambda.ts:657-662`).
  - That declaration is an env string. It does not prove the Worker gate is deployed; proving that is an operator check (section 11).

## 2. Image prerequisite

- Neither PR runs anywhere until an image containing c858e7fe (or a descendant) is published and deployed. The
  deployed 8bb18be5 excludes both.
  - The feeder lives in the session-pipeline Lambda (`lambda.ts:378-391`, `:654-676`).
  - The gate lives in the read Worker (`packages/session-read-worker/src/index.ts:104`, `:325-337`), which is deployed
    by `.github/workflows/deploy-session-read-worker.yml` (input `buffered_search_gate`, L57-58; env L97).
- The Worker deploy is a separate dispatch from the Lambda image publish (`publish-image*.yml`).
  - A Worker dispatch without the input turns the gate off again (`deploy-session-read-worker.yml:39-40`).
  - So every later Worker deploy must repeat `buffered_search_gate=on` while the feeder is on.
- `[missing]` Lambda deploy wiring for the feeder:
  - There is no EventBridge rule for the `buffered-search-feed` schedule key. The key is dispatched at
    `src/session-pipeline/lambda-runtime.ts:9,16`, but `stacks/aws/session-pipeline.ts` defines only the
    semantic-copy schedule.
  - The S3 notification filter is prefix `96bulls/` plus suffix `/manifest.json`
    (`stacks/aws/session-pipeline.ts:284-294`), so buffered stage keys never reach the consumer keep-up path
    (`src/session-pipeline/consumer.ts:46-51,81-98`).

## 3. Migration prerequisites (043860, 043870)

- Order on main: `20261105043850_ingest_acquisition_owner` < `043860_buffered_search_gate` <
  `043870_buffered_search_feed_cursor`. The migrator ledger is `sessions.__96bulls_migrations`. The bootstrap
  runbook sentinels are at `runbooks/session-directory-search-worker-bootstrap.md:397-398`.
- 043860 requirements:
  - It must run as the original creator, which owns 11 ledger tables and `owner_buffered_parity_expected`,
    with `bulls96_read_api` present. Otherwise it fails 55000 (`043860...sql:158-178`).
  - It creates no role (L149-152). It grants EXECUTE to `bulls96_read_api` only and revokes from PUBLIC/anon/
    authenticated/service_role/ingest roles (L372-375). Any other grantee is stripped (L380-396).
  - It is STABLE, takes AccessShareLock only, runs under a 2s statement_timeout, and caps at 500 hits (L142-144).
  - It must be applied before `READ_API_BUFFERED_SEARCH_GATE=on`. Otherwise every gate call fails closed (L154-157).
- 043870 requirements:
  - `SET LOCAL lock_timeout='5s'` (L59).
  - The creator must own the four ledger tables plus `ingest_buffer_gc_ordinals`, `ingest_exact_keys` and
    `ingest_bounded` (L61-82).
  - The role `buffered_search_feed_runtime` must already exist (42704 if absent). It fails 55000 if the role is
    elevated, INHERIT, or has any member other than the Supabase creator edge (L90-105).
  - It creates `sessions.buffered_search_feed_cursors` with RLS on (L107-117). EXECUTE on the three RPCs goes to
    the runtime role only (L256-264), plus USAGE on `sessions` (L267). Other grantees are stripped (L271-291).
  - Its header names the ClickHouse columns and the read gate as separate prerequisites (L53-58).
- Function-boundary lanes and ACL:
  - Lanes: `scripts/sessions-function-boundary/lanes/buffered-search-gate.ts` and
    `scripts/sessions-function-boundary/lanes/buffered-search-feed.ts:18-31`.
  - ACL: `src/hoopdb/sessions-function-acl.json:50-62`.
  - The phase-2 verifier is generated in `scripts/sessions-function-boundary/verifiers.ts:67-73` and is part of
    the derived schema build (`stacks/aws/derived-schema-build.ts:28`).
- ClickHouse DDL is `[missing]` as a reviewed artifact.
  - The only DDL is `docs/proposals/buffered-search-feed.clickhouse.draft.sql:1-8`, which adds
    `origin LowCardinality(String) DEFAULT 'canonical'` and `origin_claim_token String DEFAULT ''`. Its header
    says "an operator applies it (with Khaliq's OK) BEFORE INGEST_BUFFERED_SEARCH_FEED=on" and that the
    vendored schema is owned by relayhistory-cloud (L1-6).
  - Not stated in the draft: gate mode reads these columns with no legacy fallback.
    - `page.ts:169` goes straight to `selectPage(..., "gate")`.
    - `queries.ts:269-270` names `origin, origin_claim_token` in every mode except legacy.
    - The missing-column fallback exists only on the canonical path (`page.ts:170-183`).
    - So the ClickHouse columns must exist BEFORE the gate is turned on, not only before the feeder.
  - There is no rollback DDL for the columns. See section 7.

## 4. Role prerequisites

- `bulls96_read_api` (read Worker):
  - Existing role. The #338 bootstrap edits are `supabase/bootstrap/session_read_api_roles.sql:27-36,73-77`
    (inbound-member section and creator edge).
  - Verifier: `supabase/bootstrap/session_read_api_role_verify.sql`. The gate function is in its allowed list at
    L104. The PG16 member loop is at L49-53. It runs with `PGOPTIONS='-c hoopdb.db_mode=supabase'`.
- `buffered_search_feed_runtime` (feeder):
  - The constant is at `src/session-pipeline/buffered-feed-role.ts:9`.
  - Bootstraps create it `NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION`:
    - `supabase/bootstrap/session_ingest_roles.sql:25-34,55,88-96`
    - `cloudsql_migration_roles.sql:161-162,182`
    - `rds_migration_roles.sql:183-184,202`
    - It is reserved in `hoopdb_runtime_role_rds.sql:77`.
  - The LOGIN step is `supabase/bootstrap/buffered_search_feed_runtime_role.sql:8-17`.
    - It sets `LOGIN NOINHERIT` with statement_timeout 5s, lock_timeout 2s and
      idle_in_transaction_session_timeout 10s.
    - Its header says it is "Operator-only ... Run only under Khaliq's explicit apply OK" and that the password
      comes "separately from 1Password in memory" (L1-7).
    - The verifier `buffered_search_feed_runtime_verify.sql` must pass before and after.
- Feeder credential:
  - Secret fields are `buffered_search_feed_database_url` and `buffered_search_feed_database_ca_pem`, with
    verify-full TLS (`src/session-pipeline/buffered-feed-login.ts:1-27,43-44`; `lambda.ts:96-102`).
  - If both are absent, the feed is disarmed as `feeder_login_absent`; if either is invalid, it is disarmed as
    `feeder_login_invalid` (`lambda.ts:212-225`).
  - The feeder gets its own pool with max 1 connection and needs the ClickHouse search binding (`lambda.ts:378-391`).
- Dogpatch precondition, which is not yet proven on the real pooler:
  - The 043870 RPCs check only `session_user` and role `none` server-side (`043870...sql:143-144`; header L36-51).
  - `backend_user` and `system_user` are checked client-side by `assertBufferedFeedLogin`.
    - `backend_user` must equal the role exactly (`buffered-feed-login.ts:109`).
    - On PG16+, the identity suffix of `system_user` must equal the role (L118-129).
    - Members may be only the creator edge (L130-136).
  - The checks use `src/acquisition/db-login.ts:97-112` (LOGIN_SQL, SYSTEM_USER_SQL, MEMBERS_SQL).
  - The connection user may be `<role>.<tenant>` (`POOLER_USER`, `buffered-feed-login.ts:45`). Whether the real
    pooler then reports `backend_user`/`system_user` as the bare base role is not proven by source.
  - A mismatch fails closed (the feeder refuses), but the proof itself is an operator step (section 8, step 10).
  - 043860 likewise checks only `session_user` for `bulls96_read_api` (`043860...sql:204-206`).

## 5. Privacy binding

- With the gate on, a buffered-origin hit is emitted only if all of these hold
  (`buffered-search-gate.ts:18-56`; `docs/session-read-api-design.md:190-218`):
  - The owner's control head is fetched over `HISTORY_CONTROL_HEAD` and is not pending.
  - Original admission matches.
  - The claim binds the hit's (source, session).
  - Gate and G2 parity hold.
  - The session is not in any deletion set.
  - The batch is committed and buffered.
  - The reset count matches.
  - The admission is the latest.
- Drops and refusals:
  - The SQL reasons are listed at `043860...sql:356-367`.
  - The Worker also drops for unknown origin, invalid claim token, more than 16 owners, control-head unavailable
    or pending, and admission unavailable or missing (`buffered-search-gate.ts:396-457`).
  - With the flag on and no binding, the Worker warns and every buffered row drops (`index.ts:325-337`).
  - The binding is pinned to `relayhistory-api` / `SessionReadPrivacyControls`
    (`packages/session-read-worker/scripts/render-deploy-config.ts:50-55`). This is a cross-repo dependency on
    relayhistory-cloud.
- Workspace allowlist: the #338 render emits none.
  - The workflow says the gate "needs no workspace list and no S3" (`deploy-session-read-worker.yml:36-37`). The
    design doc says the same (`docs/session-read-api-design.md:220`), as does
    `runbooks/sessions-prod-session-read-worker.md:76`.
  - Unlike owner-buffered reads, which pass a workspace allowlist (`design.md:183-186`), the gate applies in every
    workspace the Worker serves.
  - Who becomes visible: in any workspace, any reader whose tenant-scoped search returns a buffered row whose OWNER
    passes every check above. In a shared workspace that includes other users' rows (threat model, `design.md:211-218`).
  - Gate-on alone makes nothing buffered visible: no `origin='buffered'` row exists until the feeder writes one.
- Residual divergence:
  - Not mirrored: the per-stage and per-ordinal 043700 conditions, which are outbox active stage, stage ack and
    redaction, GC ancestor chain, group/ordinal shape, and lazy ACK native slot
    (`043860...sql:122-125`; `design.md:207-208`).
  - The feeder narrows this at write time. It indexes only active, acknowledged, live outbox stages that
    `ingest_buffer_gc_ordinals` retains (`buffered-search-feed.ts:42-46`; `043870...sql:162-166,202-208`).
  - It does nothing for later changes.
- Later erasure is `[missing]`.
  - "Deletes for erasure that happens LATER (session delete/reset/GC) are a separate lane; until it lands the
    query-time check keeps deleted sessions out" (`buffered-search-feed.ts:48-51`).
  - Erased-session rows remain in ClickHouse and are hidden only by the gate (with the gate on) or by the
    canonical filter (with the gate off).
- Snapshot consistency: `READ_API_REPEATABLE_READ` is off by default, so gate chunks may see different snapshots
  (`buffered-search-gate.ts:279-286`; `design.md:222`).

## 6. Validation (source-level; not run for this packet)

The clone has no installed dependencies, so the tests below were not executed for this packet. Targeted files:
- Gate:
  - `src/session-read-api/__tests__/buffered-search-gate.test.ts`
  - `src/session-read-api/__tests__/buffered-search-gate.pglite.test.ts`
  - `src/session-read-api/__tests__/buffered-origin.clickhouse.test.ts`
  - `src/session-read-api/__tests__/app.test.ts`
- Feeder:
  - `src/session-pipeline/__tests__/buffered-search-feed.test.ts`
  - `src/session-pipeline/__tests__/buffered-search-feed.pglite.test.ts`
  - `src/session-pipeline/__tests__/buffered-feed-login.test.ts`
- Migrations, ACL, roles:
  - `src/hoopdb/__tests__/migration-profile.test.ts` (up/down/up for both downs, per their headers)
  - `src/hoopdb/__tests__/sessions-function-boundary.test.ts`
- CI coverage:
  - `ci.yml` unit-tests runs a real ClickHouse service and the package test scripts (`.github/workflows/ci.yml:24-73`).
  - `pg-proof.yml` had no job for the 043860/043870 tests or `buffered-feed-login` against real PostgreSQL on
    main at c858e7fe (L1-330). `db-login.postgres.test.ts` covers the shared installer identity queries
    (L233-237).
  - `scripts/pg-proof/role-proof.ts` (around L469) already runs
    `src/session-pipeline/__tests__/buffered-privacy.postgres.test.ts` inside the pg-proof real-postgres job, so
    the 043700 owner-buffered privacy guard already has real-PG CI on main.
  - Real-PG proof of the feed RPC login guard is `[pending #376]`. PR #376 (branch
    `ci/pg-proof-buffered-feed-gate-login-20261108`, head 5c0dc470) adds
    `src/session-pipeline/__tests__/buffered-feed-login.postgres.test.ts` and a pg-proof job
    `buffered-feed-login-postgres` (PostgreSQL 15/16/17) covering 043860+043870 up/down/up and the feed RPC login
    guard. It becomes closed when #376 merges.
- Operator-side validation, run read-only by the operator lane only:
  - Both role verifiers, before and after each role step (section 4).
  - Sentinel rows for 043860/043870 in the migration ledger.
  - `system.columns` showing exactly the two origin columns. The feeder asserts this itself (`buffered-search-feed.ts:324-333`).

## 7. Rollback

- Gate:
  - Redeploy the Worker without `buffered_search_gate`. The render is byte-identical to before
    (`deploy-session-read-worker.yml:34-35,39-40`).
  - Event pages then serve `origin='canonical'` only (`app.ts:165-169`; `queries.ts:322-333`).
- Feeder:
  - Set `INGEST_BUFFERED_SEARCH_FEED` unset or `off`. The Lambda returns before any secret is read (`lambda.ts:654-656`).
  - Already-written buffered rows stay in ClickHouse. They are hidden by the canonical filter with the gate off.
    No source deletes them in bulk `[missing]`.
- 043870 down (`supabase/rollbacks/20261105043870_buffered_search_feed_cursor.down.sql`):
  - Turn the feed off first (L8-9).
  - It is refused with 55000 while a later migration is applied (L13-14,19-24).
  - It drops the three RPCs and the cursor table (L25-28), so cursor positions are lost. A reapply restarts from
    `since`, and ClickHouse folds identical rows (L3-5).
  - It revokes schema USAGE (L30) and deletes its ledger row (L31-35). Run it with `psql -1 -v ON_ERROR_STOP=1` (L17-18).
  - The role is KEPT (L6). Any LOGIN granted by the role script stays until reverted. A NOLOGIN revert script is
    `[pending #375]`. PR #375 (branch `ops/buffered-feed-runtime-nologin-revert-20261108`, head 6a53ef07) adds
    `supabase/bootstrap/buffered_search_feed_runtime_role_nologin.sql` (`ALTER ROLE ... NOLOGIN NOINHERIT PASSWORD
    NULL` inside a transaction, refusing with SQLSTATE 55000 if the role is missing) and a runbook note that
    NOLOGIN stops new authentication only (pooled connections keep working). Treat the revert as a disarm only
    once the feeder flag is off and in-flight invocations have finished.
- 043860 down (`supabase/rollbacks/20261105043860_buffered_search_gate.down.sql`):
  - Turn the gate off first (L3-5).
  - It is refused with 55000 if a later migration such as 043870 is applied (L15-20), so roll back 043870 first.
  - It drops the function (L21) and its ledger row (L22-26).
- ClickHouse columns: there is no rollback artifact `[missing]`. Hazard derived from source:
  - If the columns were dropped while buffered rows exist, the canonical query fails on a missing column, the
    probe finds them absent, and the read falls to `legacy` mode, which serves rows unfiltered (`page.ts:174-183`).
  - Buffered rows would then be served without any gate.
  - The columns must not be dropped while any `bf`-fingerprint row exists (`buffered-search-feed.ts:77`).

## 8. Ordered checklist

Each step is an independent gate. Nothing here is executed from this packet.

1. [source-ready] #338 and #339 are on main at c858e7fe, default-off (section 1).
2. [missing] Reviewed ClickHouse origin-column DDL plus a rollback note owned by the schema owner (relayhistory-cloud). Only a draft exists (section 3).
3. [missing] An EventBridge schedule (and/or notification) for `buffered-search-feed`, and Lambda env/secret wiring, in `stacks/aws` (section 2).
4. [missing] Scope-bound feeder source if a tenant/session canary is wanted (section 10).
5. [missing] A later-erasure delete lane for buffered ClickHouse rows (section 5).
6. [pending #375/#376] NOLOGIN revert script and real-PG CI for the feed login guard (sections 7 and 8).
7. [operator: needs Khaliq OK] Publish and deploy an image containing c858e7fe. The deployed image 8bb18be5 excludes both PRs.
8. [operator: needs Khaliq OK] Apply 043860 to the real database as the original creator, then run `session_read_api_role_verify.sql`.
9. [operator: needs Khaliq OK] Apply the ClickHouse origin-column DDL. It must precede the gate (section 3).
10. [operator: needs Khaliq OK] Prove on the actual pooler, read-only, that `backend_user`/`system_user` report the base role for each login used. Any mismatch fails closed (dogpatch precondition; section 4).
11. [operator: needs Khaliq OK] Prove the G2 parity / control-head prerequisites for each owner meant to become visible (section 11).
12. [operator: needs Khaliq OK] Dispatch the read Worker deploy with `buffered_search_gate=on`. Verify the gate is live and that buffered hits drop or pass as expected.
13. [operator: needs Khaliq OK] Run the role bootstrap. Apply 043870, then run `buffered_search_feed_runtime_verify.sql` (NOLOGIN profile).
14. [operator: needs Khaliq OK] Run `buffered_search_feed_runtime_role.sql` (LOGIN). Run the verifier before and after. Load the credential from 1Password into the secret fields.
15. [operator: needs Khaliq OK] Set `BUFFERED_SEARCH_FEED_GATE_DECLARED=buffered-search-gate/v1`, choose the scope values (section 10), and flip `INGEST_BUFFERED_SEARCH_FEED=on`. Only after step 12 is verified.

Khaliq's rule applies throughout: no canary until the full privacy gate exists. Items 2-5 and the
residual divergence in section 5 bear on whether the gate is "full"; that call is not made here.

## 9. Open questions for the operator lane

1. Who owns and reviews the ClickHouse origin-column DDL and its rollback: relayhistory-cloud's vendored schema, or this repo?
2. Does the production pooler report `backend_user` and `system_user` as the bare base role for `buffered_search_feed_runtime`, and for `bulls96_read_api` through Hyperdrive?
3. Is relayhistory-cloud's `SessionReadPrivacyControls` entrypoint deployed in the version the gate parses (`design.md:168`)?
4. Do owners intended to become visible have current G2 parity rows, and is the `ingest_lazy_policy` singleton unexpired with the matching epoch and baseline (section 11)?
5. Is the residual 043700 per-stage/ordinal divergence acceptable for "full privacy gate", given that later erasure is not yet deleted from ClickHouse?
6. Should `READ_API_REPEATABLE_READ=1` accompany the gate (Hyperdrive behaviour unproven, `design.md:222`)?
7. How does the operator confirm that every later Worker deploy keeps `buffered_search_gate=on` while the feeder is on?
8. Is a NOLOGIN revert script for `buffered_search_feed_runtime` required before any LOGIN step? (script now proposed in #375; whether it must land before any LOGIN step stays Khaliq's call)

## 10. Scope binding

- Intended scope: none is configurable in source. The feeder is a broad backlog feed across tenants, NOT an exact
  own-session canary.
  - `buffered_search_feed_page` selects every org/workspace/user: `WHERE s.cleanup_state='live' AND
    s.receipt_state='acknowledged' AND s.created_at>=c.since` plus the cursor keyset and the active-outbox
    `EXISTS` (`043870...sql:161-166`).
  - Its input is exactly `cursor`, `since` and `limit` (L145). There is no org, workspace, user or session parameter.
  - `bufferedFeedConfig` reads no scope variable (`buffered-search-feed.ts:127-150`).
  - The tenant in each page row is used only for key and header binding checks (`buffered-search-feed.ts:277,400-401`).
- Defaults, quoted from source:

  | Setting | Default | Allowed range | Citation |
  | --- | --- | --- | --- |
  | `since` | `2026-11-05T05:33:45.000Z` ("the first buffered acceptance in production") | ISO-8601 UTC | `buffered-search-feed.ts:81-82` |
  | cursor | `backfill-20261105` | `^[a-z][a-z0-9-]{0,62}$` | `buffered-search-feed.ts:83`, `:139-140` |
  | `INGEST_BUFFERED_SEARCH_FEED_PAGE` | 50 | 1-200 | `buffered-search-feed.ts:144` |
  | `INGEST_BUFFERED_SEARCH_FEED_RATE` | 5 stage checks/s | 1-50 | `buffered-search-feed.ts:145` |
  | `INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS` | 50000 | 5000-240000 | `buffered-search-feed.ts:148` |

  These match the coordinator's reading.
- Values an operator would set, all env on the Lambda, none wired today (section 2):
  - The flag and the gate declaration.
  - `_SINCE` and `_CURSOR`. A cursor is bound to its `since`, and reusing a name with a different `since` is
    22023 (`043870...sql:150-153`), so a new window needs a new cursor name.
  - Optionally `_PAGE`, `_RATE` and `_BUDGET_MS`.
  - None of these restricts tenants. A later `since` only narrows by time.
- Expected workload per invocation, from `runBufferedFeed` arithmetic:
  - Stage checks are paced at `1000/rate` ms (`buffered-search-feed.ts:491`) inside a `budgetMs` deadline (L490,496).
    At the defaults that is at most about 50 s × 5/s = 250 stage checks, which is about 5 pages of 50.
  - Each check is one SQL call plus one S3 read (L99).
  - Rows per stage are unbounded by config. Memory is flushed at 64 MiB of held rows (L528).
  - A run stops early on the first `deferred`/`pending` stage (L518) or when caught up (L538).
  - The budget comment assumes a one-minute schedule (L104). No schedule exists (section 2).
  - The per-day rate is therefore not determinable from source.
- Rollback per scope: there is one global scope, so rollback is global (section 7).
  - Flag off stops new writes.
  - Gate off hides buffered rows.
  - Written rows are not removed by any source path.
  - Rolling back 043870 drops every cursor.
- Source work needed for a tenant/session-bound canary `[missing]`, for review (no input is proposed):
  - A scope parameter in `buffered_search_feed_page` (043870 or a new slot) with a matching cursor binding.
  - Scope env parsing and refusal in `bufferedFeedConfig` (`buffered-search-feed.ts:127-150`) behind its own
    default-off flag.
  - The `sqlBufferedFeedSource` call (L195-252).
  - Updates to the lane/ACL (section 3).
  - Tests in `buffered-search-feed.test.ts`, `buffered-search-feed.pglite.test.ts` and `migration-profile.test.ts`
    proving that out-of-scope stages are never paged.
- The two-batch frozen-source receipt lookups already completed by the operator lane authorize nothing here and do not approve a global backfill.

## 11. G2 parity / control-head prerequisites

Turning on the buffered gate does not, by itself, prove that any owner becomes visible. Source requires all of
the following for an owner's buffered hit to pass (`043860...sql:253-289`; `buffered-search-gate.ts:18-56`):

- Control head:
  - `HISTORY_CONTROL_HEAD` answers 200 with exactly the producer's 12-key shape and an exact tenant echo
    (`design.md:168`; `buffered-search-gate.ts:144-161`).
  - The head is not pending, and the head epoch equals the cutover epoch (`043860...sql:255`).
- Privacy authority: the `sessions.ingest_privacy_authority` singleton is not `fenced` (L256).
- Policy: the `sessions.ingest_lazy_policy` singleton is unexpired, and its `control_epoch` and
  `privacy_baseline_sha256` equal the head's (L259-263).
- G2 parity:
  - A `sessions.owner_buffered_parity` row must exist for (org, workspace, owner); otherwise the hit drops as
    `parity_missing` (L264-268). The table has no rows by default
    (`supabase/migrations/20261105043700_owner_buffered_read.sql:189-190`).
  - The full tuple must match: cutover epoch, baseline, no_head/head epoch/version/digest/kind, target generation,
    deletion/reset set digests, and target lineage. `receipts_ok` must hold, as recomputed by
    `sessions.owner_buffered_parity_expected`; otherwise the hit drops as `parity_stale` (L269-286).
  - Parity rows are written only by the root-only G1/G2 operator, which writes a row only after a fresh G1 census
    (`scripts/owner-buffered-parity-operator.ts:1-30`; `src/migration/owner-buffered-parity.ts:1-8,162`).
- Per hit:
  - Admission epoch and version match.
  - The claim binds the session.
  - The session is not deleted.
  - The batch is buffered.
  - The reset count matches.
  - The admission is not superseded (section 5).
- Publication frontier: the gate does not check it. Published exclusion (`ingest_publication_heads`) is listed as
  "Inapplicable" to the gate (`design.md:209`). Canonical dedupe on search is not the gate's job.

An owner is therefore shown to be visible only by a fresh G2 parity row matching the live control head and policy
plus a passing per-hit verdict. A read-only operator check of `buffered_search_gate_for` drop reasons for that
owner's hits would show it; source provides no such tool, so it is `[missing]` as a reviewed artifact. Gate-on
with `parity_missing` everywhere is the expected fail-closed state.

Ordering, restated:
- Default-off everywhere.
- The ClickHouse columns, then the read gate (#338), must be active and verified BEFORE the feeder (#339) is enabled.
- No production read, flag, dispatch or apply is performed or authorized by this packet.
