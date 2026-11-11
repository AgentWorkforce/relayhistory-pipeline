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
`ubuntu-latest`, not the `vars.CI_LINUX_RUNNER` expression of `pg-proof.yml`. It connects only to `postgres://postgres@127.0.0.1:5432/postgres` and the
service superuser on the same loopback socket. It is DESTRUCTIVE to the database it
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

## Stages

Each stage writes `rehearsal-out/<stage>.json` (uploaded as the job artifact) and the
first FAIL stops the run. `summary.json` lists every stage and `expect_refusal`; the
job fails unless `passed` is true. A refusal raised between stages is recorded against
the stage the run is in (or `<stage>-after` when that stage already has a record).

R3 runs after R4: it checks the boundary that R4's `beforeWrite` refreshed under the
migrator lock, so it is listed in execution order below.

| Stage | Proves |
| --- | --- |
| R0 seed | Owner, runtime and Supabase shim roles, bootstrap chain, function boundary; the baseline landing commit's schema steps applied by `runMigrations` with a release target binding; marker PASS at that commit's generation (A); the previous release's event-trigger body installed (stale wrapper). R0's bytes (shim, bootstrap chain, boundary, seed steps and the 7 verifiers) come from the baseline landing commit's tree in the Git object database, each seed step's sha256 checked against that tree and the release tree (`rehearsal_seed_bytes_drift`); R1 onward use the release sha's checkout. |
| R7-pre | (clean runs where the baseline already has the 044030 overlap) synthetic lazy policy, seat and completeness at A; old and `{B,A}` Workers admit; the owner opens the A to B overlap. Otherwise SKIP. |
| R1 plan | `inspectRelease` (READ ONLY; refuses `function_acl_drift`, `ledger_gap`, ...), `migrationTargetSplit` equal to an independently computed window, the target anchor at `generation`, `validateReleasePlan` on the plan built exactly as production builds it. |
| R2 boundary | The planned boundary is safe and is still the previous body (prosrc not exact). |
| R4 apply | Production apply order (anchor helpers `targetAnchor`/`sameAnchor` imported from `scripts/schema-release-database.ts`): manifest anchor, inspection, ledger digest, boundary digest, pending window, generation, manifest, then `runMigrations` with `beforeWrite` re-anchoring and refreshing the boundary under the migrator lock, then the production read-backs and `verifyReleaseBoundary`. |
| R3 boundary | The refreshed event-trigger body is the pinned body (`SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256`). |
| R5 read-back | Ledger = seed plus the approved window exactly; target ledgered; no later same-lane or held step ledgered; marker PASS at `generation`. |
| R6 ingest | `sessionIngestVars` with placeholder ids: during the overlap `INGEST_ACCEPT_GENERATIONS` is `A,B`; after it only `INGEST_EXPECT_GENERATION=B`. Evidence carries only those two keys. |
| R7 fence | Old and `{B,A}` admit; `rebindIngestGeneration` rebinds and closes the overlap; old is refused `ledger_sqlstate_55000`; next and `{B,A}` admit. SKIP without R7-pre. |
| R8 read | Only when 047010 is in the applied window and R7 passed: 17 mixed claims through the real ingest app, 170 groups, 6939 records; backfill and reader enabled; the target session's exact-stage read equals frozen 044090 with 17 stages and 177 kept ordinals, every `group_indexes = {0}`. Otherwise SKIP with the reason. |

## Rows to run before a release

| Row | `baseline` | `target` | `inject_failure` | Expected |
| --- | --- | --- | --- | --- |
| Release A | the production ledger tail | the Release A `RELEASE_TARGET_STEP` | `none` | R0-R7 PASS on 15/16/17; R8 SKIP `047010_not_applied` unless the window contains 047010. |
| 047010 alone (known negative) | `upstream:20261105047000_coordination_fact_readers.sql` | `upstream:20261105047010_buffered_exact_session_stages.sql` | `none`, `expect_refusal` `R1:rehearsal_target_anchor_missing` | R1 FAIL `rehearsal_target_anchor_missing`, zero apply (R4 never runs). No first-parent commit of main has an upstream lane ending at 047010: its landing commit 0f9c360d already ends at 057000. Not expressible under the release-target contract (see Candidate anchors). |
| Release C on a pre-#534 sha (negative control) | any | any | `none`, `expect_refusal` `R0:rehearsal_release_api_missing` | R0 FAIL `rehearsal_release_api_missing`: that tree has no release-target API (`release-target.ts`, `migrationTargetSplit`; first on main at #534), so only a legacy full apply is possible. The evidence lists `fullApplyPending` and `beyondTarget`. Not a real release target: do not release from such a sha. Any sha without the exported `targetAnchor`/`sameAnchor` (added with this rehearsal) also refuses this code. |
| Ledger gap | as the release | as the release | `ledger-gap` | `R1-ledger-gap` PASS: a seeded upstream row is removed and the plan refuses `ledger_gap`. |
| Boundary drift | as the release | as the release | `boundary-drift` | `R4-boundary-drift` PASS: a PUBLIC EXECUTE default appears after the plan; apply refuses `function_boundary_unproven`; ledger unchanged. |
| Late step | as the release | as the release | `late-step` | `R4-late-step` PASS: the target step fails after every earlier pending step is ledgered; marker pending at `generation`; then `R1-rerun`, R4, R3, R5 PASS (the rerun completes). |
| Wrong generation | as the release | as the release | `wrong-generation` | `R4-wrong-generation` PASS: a flipped target generation fails `validateReleasePlan` (`release_plan_artifact`) and `runMigrations` refuses `release_target_generation_changed` before any write; ledger, marker and boundary unchanged. |
| Stale plan | as the release | as the release | `stale-plan` | `R4-stale-plan` PASS: the ledger changes after the plan; production `runMigrations` (given the plan's stale ledger digest, as `releaseDatabase` passes it) refuses `release_ledger_changed` under the migrator lock before `beforeWrite`, and the release script order replayed by the driver refuses it too; nothing else written. |

Run every row on all three majors (the dispatch matrix). Attach the `summary.json`
artifacts to the release review. Pull requests touching the release path run one smoke
row automatically (PostgreSQL 17, baseline 045030, target 047000).

Injection rows expect their own stage to PASS, so leave `expect_refusal` empty for them.

## Candidate anchors

For information only; this section selects nothing. First-parent commits of main whose
schema lanes could serve as a release target commit:

| Commit | Upstream tail | Note |
| --- | --- | --- |
| 113dc657 | 047000 | Release A anchor (the PR smoke generation `52a3f2f0...`). |
| 98558d9f | 050010 | No 047010. |
| 3e2fd7c7 | 056000 | |
| 8f8ceaeb | 057000 | |
| 0f9c360d and later | 057000 | With 047010. |

NOT EXPRESSIBLE under the release-target contract: standalone 047010 (no commit ends
the upstream lane at 047010) and "Release C pre-#534" (no release-target API). Any
folded or branch-anchored release needs a separate amendment and its own approval. A
rehearsal of any of these rows is prerequisite proof only and authorizes nothing.

## Limits

- The rehearsal checks the release mechanics against a reconstructed database, not a
  production snapshot. Data-dependent behaviour (row counts, locks under load, the
  real event-trigger body) is not covered.
- The production GitHub release gate (approval re-check in `beforeWrite`) is not
  exercised; there is no approval to re-check.
- The seed uses the release sha's current verifiers with the baseline commit's prefix.
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
