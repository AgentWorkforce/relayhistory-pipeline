# Schema release rehearsal (disposable PostgreSQL)

`.github/workflows/rehearse-session-schema.yml` replays a guarded schema release
(`runbooks/guarded-session-schema-release.md`, `scripts/schema-release-database.ts`)
against the job's own throwaway `postgres:15`, `postgres:16` and `postgres:17`
services. A PASS is prerequisite proof only and authorizes nothing: it does not
approve, start or substitute for any apply, deploy, rebind or flag change on a real
database, and every such step still needs its own explicit owner approval.

The workflow has `permissions: contents: read`, no environment, no secrets and no
production URL. The driver (`scripts/pg-proof/schema-rehearsal.ts`) refuses to run
unless `GITHUB_ACTIONS=true`, `REHEARSAL_DISPOSABLE_PG=1` and
`RUNNER_ENVIRONMENT=github-hosted` (`rehearsal_not_disposable_ci`,
`rehearsal_not_github_hosted`); no libpq connection variable (any `PG*` connection
setting), `DATABASE_URL` or `SUPABASE_*URL*` is set (`rehearsal_connection_env_set`);
the checkout is the exact clean full-history release sha; and the database is fresh:
no `sessions` schema, no owner role, and `pg_postmaster_start_time()` within the last
60 minutes (`rehearsal_database_not_fresh`). Both jobs therefore run on GitHub-hosted
`ubuntu-latest`, not the `vars.CI_LINUX_RUNNER` expression of `pg-proof.yml`. It connects only to a passwordless role on
`127.0.0.1:5432` and the service superuser on the same loopback socket. It is DESTRUCTIVE to the database it
runs against; never point it anywhere else.

## Inputs

| Input | Meaning |
| --- | --- |
| `sha` | Release commit (40 lowercase hex). Checked out with full history; its own `migrate.ts`, `release-target.ts`, `schema-release-*.ts` and boundary modules run. |
| `baseline` | The last step the production ledger holds today, lane-prefixed (`upstream:<file>.sql`). The seed is exactly the schema steps of the first-parent commit that added this file. |
| `target` | `RELEASE_TARGET_STEP` for the release. |
| `generation` | The 64-hex target generation the apply must mark PASS. The target commit is found on the release sha's first-parent history at this generation. |
| `inject_failure` | `none` for a clean apply, or one refusal to prove (below). |
| `expect_refusal` | Empty (default): the run passes only if no stage FAILs. `<stage>:<code>` (for example `R1:rehearsal_target_anchor_missing`) marks a known-negative row: the run passes only if that stage FAILs with exactly that code and no later stage ran; any other outcome, including a clean pass, fails the job. It does not change `inject_failure`. |
| `observe_turns_lock_hold` | Default false. On a clean row only, start the read-only 062020 relation-lock observer before R0 and require its exact ledger receipt and threshold result. Its JSONL is uploaded as `turns-lock-hold.jsonl`. |

## Turns-forward lock-hold evidence

`scripts/pg-proof/lock-hold-observer.ts` is a read-only observer for
`20261105062020_turns_forward_gates.sql`. It runs in a separate session with
`default_transaction_read_only=on`, a one-second statement timeout and a one-second
lock timeout. The database URL is read only from `OBSERVER_DATABASE_URL`; never put it
in argv or logs. Non-loopback URLs require a password. The observer emits relation
names, backend PIDs, modes and durations, but never query text, usernames, database row
contents or the URL.

For the disposable workflow, set `observe_turns_lock_hold=true`. The observer connects
first, waits read-only while R0 creates the target relations, and then samples every
25 ms while the rehearsal runs in the same workflow step. The row is valid only when
`baseline` is earlier than 062020 and `target` includes 062020. Success requires the exact
ledger row and frozen checksum for 062020. The uploaded evidence reports maximum hold
per table, maximum migration and writer waits, maximum concurrent waiters and distinct
blocked writers for:

- `sessions.upload_deletions`
- `sessions.operator_user_erasures`
- `sessions.session_directory`

For a separately approved dev rehearsal, start the observer before the chart migrations
Job from a host or one-off pod with database network access. The supplied role must be
read-only, able to read the migration ledger and able to observe the migrator in
`pg_stat_activity` (for example through an operator-reviewed monitoring role). Use
the database's direct PostgreSQL endpoint, not a transaction/session pooled endpoint.
Every non-loopback URL must include a password and `sslmode=require`, `verify-ca` or
`verify-full`. Confirm the target ledger starts below 062020 and the approved
migrations Job target includes 062020 before starting the observer. Use
`--until-file` for the orchestrator's completion signal and keep stdout as the JSONL
artifact. The observer identifies 062020 by its SQL slot/file marker; it also accepts a
future `hoopdb-migrate-062020%` application name. It performs no apply and authorizes no
database action.

The result fails closed when the ledger is absent or has another checksum, the migrator
is not observed, the orchestrator signals completion before the ledger, any migration
lock wait or blocked writer exceeds 5 seconds, or a migration-held target relation lock
exceeds 120 seconds. Durations are first-seen/last-seen measurements and therefore have
up to one polling interval of sampling uncertainty. `blocked_writer_count` includes
only backends requesting a relation-level writer lock; read waiters still contribute to
`max_waiters`. This evidence gates the rehearsal row. It does not cancel the migrations
Job itself. Numbers from the empty disposable database prove the observer and ordering,
but are not representative of a populated dev or production-sized database.

## Stages

Each stage writes `rehearsal-out/<stage>.json` (uploaded as the job artifact) and the
first FAIL stops the run. `summary.json` lists every stage and `expect_refusal`; the
job fails unless `passed` is true. A refusal raised between stages is recorded against
the stage the run is in (or `<stage>-after` when that stage already has a record).

R3 runs after R4: it checks the boundary that R4's `beforeWrite` refreshed under the
migrator lock, so it is listed in execution order below.

| Stage | Proves |
| --- | --- |
| R0 seed | Owner, runtime and Supabase shim roles, bootstrap chain (the landing commit's own `SESSION_BOOTSTRAP_FILES` list, read from its module text as data; the release list may only add files, `rehearsal_seed_bootstrap_chain` otherwise), function boundary (all from the baseline landing commit's bytes; that commit is the seed commit described in "R0 seeds with the landing migrator"); then the landing commit's own migrator applies the baseline (see "R0 seeds with the landing migrator" below); the release code checks the ledger is exactly the landing tree's steps and digests (`rehearsal_seed_ledger`, `rehearsal_seed_ledger_digest`), those digests equal the release tree's (`rehearsal_seed_bytes_drift`), the marker is PASS at A (`rehearsal_seed_marker`) and A equals the landing `--generation` (`rehearsal_landing_generation_mismatch`); then the previous release's event-trigger body is installed (stale wrapper). R1 onward use the release sha's checkout. |
| R7-pre | (clean runs where the baseline already has the 044030 overlap) synthetic lazy policy, seat and completeness at A; old and `{B,A}` Workers admit; the owner opens the A to B overlap (B is the target generation, so production's overlap gate passes; TTL 3600 s covers R1 through the R7 rebind). In the `ingest-overlap-missing` row the window is withheld. Otherwise SKIP. |
| R1-prebuild | The operator's out-of-band index prebuild before the plan: every `ADOPTABLE_INDEXES` index (src/hoopdb/migration-sentinels.ts) of a pending, not-held window step that is absent while its table is populated (for example 058000's after R7-pre's rows) is built as the owner with `CREATE INDEX CONCURRENTLY` from the release checkout's own declaration, one autocommit statement each, and must read back adoptable (`index_prebuild_unproven` otherwise). Evidence: the steps and the built index names. Production does the same before its plan (docs/out-of-band-index-prebuild.md; 058000 through index-prebuild-058000.yml). SKIP when no such step is pending. |
| R1 plan | `inspectRelease` (READ ONLY; refuses `function_acl_drift`, `ledger_gap`, ...), `migrationTargetSplit` equal to an independently computed window, the target commit (the release anchor on an anchor-aware release sha, see below) bound by production's `targetAnchor` at `generation`, production's buffered-ingest overlap gate (`inspectIngestOverlap`; a no-op without R7-pre's lazy policy; the reviewed Worker pair is the fixture `{from: A, to: B}` that R6 deploys, not the release sha's `PRIVATE_INGEST_OVERLAP`), `validateReleasePlan` on the plan built exactly as production builds it. |
| R2 boundary | The planned boundary is safe and is still the previous body (prosrc not exact). |
| R2-bootstrap | The release tree's bootstrap chain (`SESSION_BOOTSTRAP_FILES`, the release sha's bytes) applied before the apply, including files the release list added after the landing commit; only `bulls96_analysis_admit` and the roles `SESSION_BOOTSTRAP_ROLES` assigns to an added file may appear, bare, with no memberships, and each added file's roles must exist afterwards (`rehearsal_bootstrap_role_missing`); no other role changes (see "R2-bootstrap" below). SKIP in the `missing-bootstrap` row. |
| R4 apply | Production apply order (anchor helpers `targetAnchor`/`sameAnchor` imported from `scripts/schema-release-database.ts`): manifest anchor, inspection, ledger digest, boundary digest, pending window, generation, manifest, then the overlap gate, then `runMigrations` with `beforeWrite` re-anchoring, rechecking the overlap gate and refreshing the boundary under the migrator lock, then the production read-backs and `verifyReleaseBoundary`. |
| R3 boundary | The refreshed event-trigger body is the pinned body (`SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256`). |
| R5 read-back | Ledger = seed plus the approved window exactly; target ledgered; no later same-lane or held step ledgered; marker PASS at `generation`. |
| R6 ingest | `sessionIngestVars` with placeholder ids: during the overlap `INGEST_ACCEPT_GENERATIONS` is `A,B`; after it only `INGEST_EXPECT_GENERATION=B`. Evidence carries only those two keys. |
| R7 fence | Old and `{B,A}` admit; `rebindIngestGeneration` rebinds and closes the overlap; old is refused `ledger_sqlstate_55000`; next and `{B,A}` admit. SKIP without R7-pre. |
| R8 read | Only when 047010 is in the applied window and R7 passed: 17 mixed claims through the real ingest app, 170 groups, 6939 records; backfill and reader enabled; the target session's exact-stage read equals frozen 044090 with 17 stages and 177 kept ordinals, every `group_indexes = {0}`. Otherwise SKIP with the reason. |

### R2-bootstrap: the operator's pre-apply chain refresh

R0 applies the bootstrap chain at the baseline landing commit's bytes, which is what
production ran then. A release step may grant to a role that only the RELEASE tree's
chain creates: 046000 grants `sessions.analysis_request_v1` to `bulls96_analysis_admit`,
which `supabase/bootstrap/session_read_api_roles.sql` provisions (bare NOLOGIN) only since
05008dc6. The migrator never creates roles, so without the refresh the apply fails at
046000 with SQLSTATE `42704` (undefined_object) after the earlier pending steps commit.
The same holds for a whole chain file the release list added after the landing commit:
`supabase/bootstrap/legacy_export_privacy_roles.sql` (role
`bulls96_legacy_export_privacy`) is in the release list but not in the list of the commit
that seeds 062010, and 062030 verifies that role. R0 seeds with the landing list only; the
added files are reported in R0's evidence (`bootstrapChain.added`) and applied here.

In production that refresh is a gated, idempotent CI stage of the guarded release
workflow with its own typed authorization (it reads back first, skips when the role
already exists exactly, and the plan shows it). That stage is a separate change; until it
exists such an apply fails closed with `42704`
(`runbooks/guarded-session-schema-release.md`). R2-bootstrap models that CI stage.

It runs after R2 and before R4. For each release `SESSION_BOOTSTRAP_FILES` file (the
landing files and the added ones, in release order) it reads the
release sha's bytes from the Git object database (never the working tree;
`rehearsal_bootstrap_bytes_missing` when absent), snapshots `pg_roles` (names and
attribute flags only), applies every file as the owner exactly as R0 does (owner
SUPERUSER only for the chain, `hoopdb.db_mode` unset), snapshots again and reads the
`pg_auth_members` edges of the expected new roles. The expected new roles are
`bulls96_analysis_admit` plus, for each added file, the roles the release tree's
`SESSION_BOOTSTRAP_ROLES` (src/hoopdb/session-bootstrap-roles.ts, unit tested equal to each
file's CREATE ROLE literals) lists for it; an added file without a map entry adds none.
It refuses:

- `rehearsal_bootstrap_unexpected_role`: a new role other than the expected ones,
  or any new role while every file is byte-identical to the landing chain and none was
  added;
- `rehearsal_bootstrap_side_effect`: a role removed, a pre-existing role's flags
  changed, or the `pg_roles` row count not before plus created;
- `rehearsal_bootstrap_role_missing`: a role the release's `SESSION_BOOTSTRAP_ROLES`
  assigns to an added file is absent after the refresh (every chain file creates each of
  its roles whenever it is absent, or raises, so a correct refresh never leaves one
  absent; 062030 would otherwise fail later, in R4);
- `rehearsal_bootstrap_role_contract`: an expected role (new or pre-existing) not
  exactly NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION,
  or with any inbound member or outbound membership.

Evidence: `files` (`file`, `release_sha256`, `landing_sha256`, `changed`),
`roles_created` (sorted names), `applied`, `roles` (for each expected role:
`present`, `preexisting`, its attribute flags, `members_in`, `member_of`; `present: false`
never for an added file's role), `role_count`
and `added` (the chain files the release list added after the landing commit). Never a
password, setting or URL.

### R0 seeds with the landing migrator

The baseline database is what production's migrator built when the baseline landed, so
R0 runs that migrator, not the release's. The release migrator cannot seed a prefix:
`runMigrations` ends in `hardenAndCheck`, whose `installRuntimeApplicationAcl` grants on
every relation in the release's `runtimeApplicationAclCatalog`, including relations
created by steps after the baseline (for 045030, the `session_analysis_*` tables of
046000), and fails with `42P01`.

R0 finds the seed commit on the release sha's first-parent history: walking forward from
the commit that added the baseline through the commits that touch either lane directory,
the newest commit that changed the baseline file before the first commit whose two lanes
differ from the add's (by step name and blob) in anything but the baseline's own blob
(`rehearsal_landing_missing` when no first-parent commit added it). A baseline amended
after its add while no other step of either lane changed (a review round on its own pull
request) is therefore seeded with its final bytes, which is what production applied. R0
seeds the seed commit's whole two-lane tree, so the walk stops at any other change: the
next step of the baseline's lane, a step inserted below the baseline, a step added to the
other lane, or an edit to an already landed step. An edit to the baseline made after such
a change (even one later reverted) is never a seed candidate, so such a seed's bytes
differ from the release tree's and R0 refuses `rehearsal_seed_bytes_drift` (landed
migrations are frozen), and a step inserted below the seed's lane tail refuses
`rehearsal_seed_noncontiguous`. R0's evidence records the seed (`landingSha`), the commit
that added the baseline (`addedSha`) and whether they differ (`seedDiffersFromAdd`). With
no candidate the seed is the add, and R0 refuses
when that commit's tree already has a later step in the baseline's lane
(`rehearsal_landing_tree_above_baseline`). The release-anchor discovery is unchanged: it
still bounds its search at the commit that added the step. R0 extracts the seed commit with
`git archive` into a temporary directory (`rehearsal_landing_materialize`), runs
`bun install --frozen-lockfile` there (`rehearsal_landing_install`), and makes the
sources read-only. It then runs `bun <dir>/src/hoopdb/migrate.ts --generation` (must equal
the release code's A) and `bun <dir>/src/hoopdb/migrate.ts` with only `PATH`, `HOME`,
`HOOPDB_URL` (the owner, `postgres`) and the production profile: `HOOPDB_DB_MODE=supabase`,
`HOOPDB_SUPABASE_SHIM=false`, `HOOPDB_ROLE_BOOTSTRAP=false`, `HOOPDB_ROLE_VERIFY=true`,
`HOOPDB_RUNTIME_ROLE`, `HOOPDB_RUNTIME_ROLE_LOGIN=true`,
`HOOPDB_FUNCTION_BOUNDARY_MODE=global-default-revoke`. A non-zero exit refuses
`rehearsal_landing_migrator_failed`. Its stdout and stderr are kept redacted (URLs and
secret-shaped text removed, last 80 lines) in `summary.json` as `landing_migrator`, and
`seed_generation` records A. The extra install shares the runner's bun cache with the
release install and fits the job's step budget.

Known limitation: a release target that HOLDS a step creating a relation listed in the
release's `runtimeApplicationAclCatalog` fails closed with `42P01` in `hardenAndCheck`
during R4 (the held relation never exists). Release A's held steps (047010, 050000,
050010, 050020, 056000, 057000 and 96bulls 0005) create none of them (only
`session_erasure_*` relations, which are not in the catalog); 046000 is inside its prefix.
A future row that holds such a step needs a separate change before it can rehearse.

## Rows to run before a release

| Row | `baseline` | `target` | `inject_failure` | Expected |
| --- | --- | --- | --- | --- |
| Release A | the production ledger tail | the Release A `RELEASE_TARGET_STEP` | `none` | R0-R7 PASS on 15/16/17; R8 SKIP `047010_not_applied` unless the window contains 047010. |
| 047010 alone, release sha without the anchor API (known negative) | `upstream:20261105047000_coordination_fact_readers.sql` | `upstream:20261105047010_buffered_exact_session_stages.sql` | `none`, `expect_refusal` `R1:rehearsal_target_anchor_missing` | R1 FAIL `rehearsal_target_anchor_missing`, zero apply (R4 never runs). No first-parent commit of main has an upstream lane ending at 047010: its landing commit 0f9c360d already ends at 057000. On a release sha with the anchor API this target is Release B (next row). |
| Release B (anchor) | `upstream:20261105047000_coordination_fact_readers.sql` | `upstream:20261105047010_buffered_exact_session_stages.sql` | `none` | R1 binds `release/schema-047010` (evidence `anchorSource`: `ref` when the checkout has the protected branch, else `fixture`: A then B built locally off 113dc657) through production's `targetAnchor`; R0-R7 PASS. |
| Release C (anchor) | as the release | `upstream:20261105050010_*` (the release's 050010 step) | `none` | R1 binds `release/schema-050010` (A, B, then the 050000 and 050010 anchors when built as a fixture); R0-R7 PASS. |
| Release C on a pre-#534 sha (negative control) | any | any | `none`, `expect_refusal` `R0:rehearsal_release_api_missing` | R0 FAIL `rehearsal_release_api_missing`: that tree has no release-target API (`release-target.ts`, `migrationTargetSplit`; first on main at #534), so only a legacy full apply is possible. The evidence lists `fullApplyPending` and `beyondTarget`. Not a real release target: do not release from such a sha. Any sha without the exported `targetAnchor`/`sameAnchor` (added with this rehearsal) also refuses this code. |
| Ledger gap | as the release | as the release | `ledger-gap` | `R1-ledger-gap` PASS: a seeded upstream row is removed and the plan refuses `ledger_gap`. |
| Boundary drift | as the release | as the release | `boundary-drift` | `R4-boundary-drift` PASS: a PUBLIC EXECUTE default appears after the plan; apply refuses `function_boundary_unproven`; ledger unchanged. |
| Late step | as the release | as the release | `late-step` | `R4-late-step` PASS: the target step fails after every earlier pending step is ledgered; marker pending at `generation`; then `R1-rerun`, R4, R3, R5 PASS (the rerun completes). |
| Wrong generation | as the release | as the release | `wrong-generation` | `R4-wrong-generation` PASS: a flipped target generation fails `validateReleasePlan` (`release_plan_artifact`) and `runMigrations` refuses `release_target_generation_changed` before any write; ledger, marker and boundary unchanged. |
| Missing bootstrap | as the release (window contains 046000) | as the release | `missing-bootstrap` | `R2-bootstrap` SKIP; `R4-missing-bootstrap` PASS: the apply refuses `42704` at the first step granting to a role only the release chain creates, the target is not ledgered; then `R2-bootstrap-rerun`, `R1-rerun`, R4, R3, R5 PASS. Refuses `rehearsal_injection_unavailable` when the chain is unchanged from landing or the role already exists. |
| Stale plan | as the release | as the release | `stale-plan` | `R4-stale-plan` PASS: the ledger changes after the plan; production `runMigrations` (given the plan's stale ledger digest, as `releaseDatabase` passes it) refuses `release_ledger_changed` under the migrator lock before `beforeWrite`, and the release script order replayed by the driver refuses it too; nothing else written. |
| Tampered anchor | as an anchored release (held steps) | as the release | `anchor-tampered` | `R1-anchor-tampered` PASS: the anchor tip is replaced by a commit whose bootstrap chain file has other bytes; the plan refuses `release_anchor_file_drift`. |
| Side parent / nonlinear anchor | as an anchored release | as the release | `anchor-nonlinear` | `R1-anchor-nonlinear` PASS: the anchor tip gains a second parent (the release sha); the plan refuses `release_anchor_nonlinear`. |
| Extra file in the anchor | as an anchored release | as the release | `anchor-extra-file` | `R1-anchor-extra-file` PASS: the anchor tip adds a path outside the allowlist; the plan refuses `release_anchor_extra_file`. |
| Missing verifier | as an anchored release | as the release | `anchor-missing-verifier` | `R1-anchor-missing-verifier` PASS: the anchor tip drops a phase 2 verifier file; the plan refuses `release_target_verifier_missing`. |
| Anchor ref movement | as an anchored release | as the release | `anchor-ref-moved` | `R4-anchor-ref-moved` PASS: after a clean plan the anchor branch moves to a twin commit (same tree and parent); the apply's first anchor recompute refuses `release_anchor_ref_mismatch` before connecting; ledger, marker and boundary unchanged. |
| Missing ingest overlap | as the release (overlap installed at the baseline, 044030) | as the release | `ingest-overlap-missing` | R7-pre installs the lazy policy at A but opens no window; `R1-ingest-overlap-missing` PASS: production's overlap gate (`src/hoopdb/schema-release-overlap.ts`) refuses the plan `release_ingest_overlap_required`, then the window A -> B is opened and R1 plans clean; `R4-ingest-overlap-missing` PASS: the window is closed after the plan and the apply refuses `release_ingest_overlap_required` before the migrator; ledger, marker and boundary unchanged. Refuses `rehearsal_injection_unavailable` when the baseline has no overlap table or the release sha has no gate. |

Run every row on all three majors (the dispatch matrix). Attach the `summary.json`
artifacts to the release review. Pull requests touching the release path run one smoke
row automatically (PostgreSQL 17, baseline 045030, target 047000).

Injection rows expect their own stage to PASS, so leave `expect_refusal` empty for them.
The anchor injections need held steps and a release sha with the anchor API
(`releaseAnchorBinding`); otherwise they refuse `rehearsal_injection_unavailable`. They
mutate only the disposable checkout's local `refs/remotes/origin/release/schema-<slot>`
ref (the protected branch when present, or the fixture); nothing is pushed.

### The target commit under the release anchor

On a release sha with the anchor API, a held-step target commit must be the protected
`release/schema-<slot>` anchor (runbooks/guarded-session-schema-release.md, "Release
anchor"). R1 uses the branch tip from the full checkout when it exists (`anchorSource`
`ref`). Otherwise it builds a fixture chain in the disposable checkout only
(scripts/pg-proof/schema-rehearsal-core.ts `buildAnchorFixture`, `anchorSource`
`fixture`): off the newest first-parent main commit (per verifier generation, down to the
baseline's landing commit) whose lane steps are all prefix steps at the release bytes, a
bootstrap-only anchor on that commit's newest target-lane slot, then one anchor per
missing target-lane prefix step with a unique slot, up to the target, each at the release
bytes (steps, their rollbacks, bootstrap files). It keeps the chain whose generation
equals `generation` and removes the others; an existing ref is never overwritten
(`rehearsal_anchor_fixture_conflict`). The plan then binds the tip through production's
own `targetAnchor`, so every `release_anchor_*` refusal surfaces unchanged.

## Candidate anchors

For information only; this section selects nothing. First-parent commits of main whose
schema lanes could serve as the main base of a release anchor (a held-step target_sha is
always the protected anchor tip, never one of these commits):

| Commit | Upstream tail | Note |
| --- | --- | --- |
| 113dc657 | 047000 | Main base of Release A: A = `release/schema-047000` with parent 113dc657, carrying #546's chain bytes (the bootstrap chain files at the release sha's bytes) and nothing else (the PR smoke generation `52a3f2f0...`). |
| 98558d9f | 050010 | No 047010. |
| 3e2fd7c7 | 056000 | |
| 8f8ceaeb | 057000 | |
| 0f9c360d and later | 057000 | With 047010. |

Under the release anchor, standalone 047010 is Release B (`release/schema-047010`, parent
the A tip) and 050010 is Release C (`release/schema-050010`, parent the B tip); see
runbooks/guarded-session-schema-release.md "Release anchor". NOT EXPRESSIBLE: "Release C
pre-#534" (no release-target API), and any held-step release from a release sha without
the anchor API. A
rehearsal of any of these rows is prerequisite proof only and authorizes nothing.

## Limits

- The rehearsal checks the release mechanics against a reconstructed database, not a
  production snapshot. Data-dependent behaviour (row counts, locks under load, the
  real event-trigger body) is not covered.
- The production GitHub release gate (approval re-check in `beforeWrite`) is not
  exercised; there is no approval to re-check.
- The seed uses the baseline seed commit's migrator, verifiers, steps and bootstrap
  chain list; its generation is that commit's canonical generation (A). Chain files the
  release list added later are applied only by R2-bootstrap, at the release bytes.
- Plan items not covered (out of scope here, not implemented):
  - the boundary `42501` negative;
  - the 047010 rollback `.down.sql`;
  - R5 `--activation-check`;
  - the R6 offline private-ingest-ci render and its wrong-pin case;
  - R2 preflight without refresh failing `function_boundary_unproven`.
- The seed is reconstructed from the baseline landing commit's schema steps, not a
  production schema snapshot.
- `ledger-gap` deletes a mid-seed ledger row; it does not simulate a later step
  (for example 050000) already present.
- `stale-plan` uses a non-schema ledger row, so only the ledger digest changes. Its
  refusal comes from production `runMigrations`; the earlier `releaseDatabase` check
  is replayed by the driver in production order, not called (it is not exported).
