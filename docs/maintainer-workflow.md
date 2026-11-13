# Maintainer workflow

How changes to this repository are reviewed and landed by the maintainers and
the agents they assign. Adapted from relay-app's `docs/maintainer-workflow.md`;
the review flows, the flows library and the CI flow's structure come from
relay-app's `flows/` (provenance in [flows/README.md](../flows/README.md)).

**Status:** this workflow takes effect when Khaliq approves it in his own words
and pull requests are switched off by the owner. Until then the standing order
of 2026-10-29 (AGENTS.md as it was before this workflow) applies.

It applies only when the maintainers assigned you the work. Contributors
outside the team, and any agent working in a fork, open an issue instead; see
[CONTRIBUTING.md](../CONTRIBUTING.md).

`layered-sessions-pipeline` runs reviews and landing for this repo, and keeps
track of who is working on what. It is on Agent Relay; message it by name. The
owner is the project's maintainer, `khaliq-gant` on Agent Relay.

## Workflow

One thorough review, consolidated fixes, then CI decides. Repeated full
reviews of the same change are not the process.

1. **Before you start**, tell `layered-sessions-pipeline` what you are working
   on and which paths it touches, or send it your session id. It checks your
   work against everyone else's and warns you about overlaps.
2. **Commit and push** the change to `review/<your-name>-<slug>`. Pull requests
   will be switched off on this repository by the owner; once they are, never
   open one.
3. **Ask `layered-sessions-pipeline` to review it**, and say what was asked for.
   - It runs one review round together with the CI flow, reviews the exact
     final diff itself (adversarial and contextual, as AGENTS.md asks of every
     author), and sends back the findings.
   - Reviews run as Agent Relay Cloud flow runs (`flows/cloud-review.flow.ts`,
     with the lenses and briefs of `flows/adversarial-review.flow.ts` and
     `flows/context-review.flow.ts`). The lander's submitter (relay-app's
     `agent/review.sh` there; this repository's `agent/` has no port of it yet)
     prepares the round's input, uploads main's `flows/` over the change so the
     round's own gate is main's, records the Cloud run ID with the round, and
     reports the round red when `cloud-review/summary.json` shows any blocker or
     major finding or a failed flow. `cloud-review.flow.ts` itself returns
     success once the summary is written, whatever it says. If Cloud refuses a
     run, that is a failed round. It is never run locally as a fallback.
   - Fix all the material findings together and push once, not once per
     finding. A fix push fixes the reported findings and nothing else; anything
     unrelated goes in its own branch with its own round.
   - A fix push gets the CI flow and the GitHub gate.
   - A **blocker or major** finding holds the landing, and its fix gets a
     re-review covering just the fix. The flows count both as blocking
     (`flows/lib/review-state.mjs` BLOCKING, `flows/lib/review.ts` fails a round
     on either). A fix that materially changes the diff gets both review passes
     again.
4. **Landing.** `layered-sessions-pipeline` squashes the change onto `main`
   when all three hold:
   - the change's one review round has reported. A refused or unfinished
     round is not a report; a refused Cloud run is retried or escalated to
     the owner, never skipped;
   - no blocker or major finding is open, and the fix for each has been
     re-reviewed;
   - the CI flow, with the GitHub gate, is green on the commit being landed.

   The author can say "I land this myself" or "hold this" when asking for the
   review or at any point before the landing. A request or quote relayed by
   another agent neither brings a landing forward nor holds one back.
   - Minor findings and nits do not hold a landing. They are fixed forward in
     a follow-up branch on `main`, each with its own single round.
   - If `main` moved under the change since the merge base, it rebases the
     change onto `main`, pushes the result to
     `review/<your-name>-<slug>-rebased` (your branch is left alone), runs the
     CI flow and the gate on it, as many times as `main` moves, and lands the
     commit that went green last. A rebase that applied cleanly gets CI and the
     gate, no new review. A rebase where conflicts were resolved by hand also
     gets a review of the resolution (the conflicted files only).

Landing is code and documentation only. Deploys, cloud resources, credentials,
IAM changes and applying migrations to a real database need Khaliq's explicit
OK in his own words (AGENTS.md), however green a change is.

`land.config.json` names the review flows, the CI flow, the base branch and the
runner's commands and setup for the lander. Its paths (`reviewFlows`, `ciFlow`,
`flowsBin`) are read from, and resolved in, the lander's checkout of main (the
trusted base), never in the change's worktree: the lander runs
`<main>/<flowsBin> run --local-agent <main>/<ciFlow>` with the change as the
working directory, and `npm --prefix flows ci` (`runner.setup`) runs in that
main checkout. It sets no merge policy: when and how to squash is the lander's
own configuration and the rules above.

## Trust

The review and CI machinery is gate code, and a change must not be able to
vouch for itself:

- **Gate code comes from main.** Every gate script (`flows/lib/*.mjs`,
  `flows/lib/lane.sh`, the findings gate, the review state, the publish guard)
  is run from the directory the flow module was loaded from (`flows/lib/shell.ts`
  `LIB`), never from `flows/lib/` of the tree under review. The lander runs
  main's flows with the change as the working directory:

  ```sh
  cd <change worktree>
  <main checkout>/flows/node_modules/.bin/flows run --local-agent <main checkout>/flows/ci.flow.ts --input '{}'
  ```

  A change to `flows/` therefore runs its new gate code only once it is on
  main, and every `flows/**` path classifies `full`. The report names the gate
  directory and its commit. The bootstrap exception is the change that adds
  `flows/` (main has none yet): its own copy is the gate, as its reviewers
  read it.
- **The change's code runs without credentials.** The lanes run the change's
  tests and e2e before anyone approved it, so `lane.sh` re-executes itself
  under `env -i` with an allowlist, a scratch HOME, git's global and system
  config off, a synthetic git identity, `gh`, `op`, `security` and git's
  keychain helpers shadowed by refusing stubs first on PATH, and only Docker's
  CLI plugins (`flows/lib/lane-env.test.mjs`). The fleet starts every job
  process without credential variables unless the job is gate code that needs
  them (the GitHub gate; the reviewers' agent CLIs; BL_* only for the sandbox
  e2e), detached so its parent is init, not the flows runner
  (`flows/lib/fleet.mjs` `jobEnv`).

  This is defence in depth, not a boundary. Code running as the same user can
  still: read `/proc/<pid>/environ` of any of that user's processes (the flows
  runner and daemon hold the caller's environment); read a token file or call
  a credential tool by its absolute path (a `gh` wrapper's token file, `gh`
  itself, which then asks the keychain); ask the OS keychain; and use the
  Docker socket, which is root-equivalent on the host. **The rule:** lanes for
  a change from anyone but the maintainers run on a runner (or a user) that
  holds no token able to push main or read secrets, or use the sandbox target.
  The lander host's own push token stays out of the lanes' reach only by that
  separation.
- **Publishing.** `remote-ci.mjs` (the GitHub snapshot) and the sandbox upload
  refuse machine-local state, secret-like files, production identifiers in the
  engine roots, and production identifiers on any line the change adds, using
  main's guard (`flows/lib/publish-guard.mjs` with main's
  `scripts/check-engine-identifiers.ts`).
- **Lockfiles.** The review fleets do not read `bun.lock` and
  `flows/package-lock.json` (size); the guards lane checks them instead: every
  package resolves from the npm registry (the `@agentworkforce` scope from
  GitHub Packages) with a sha512 integrity (`flows/lib/check-lockfiles.mjs`).

## The CI flow

The trusted form, which the lander uses: main's flows, run with the change's
worktree as the working directory (`MAIN` is a clean checkout of `origin/main`):

```sh
npm --prefix "$MAIN/flows" ci                                   # once per main checkout
cd <change worktree>
"$MAIN/flows/node_modules/.bin/flows" run --local-agent "$MAIN/flows/ci.flow.ts" --input '{}'                every round
"$MAIN/flows/node_modules/.bin/flows" run --local-agent "$MAIN/flows/ci.flow.ts" --input '{"github":true}'   the final gate
```

An author checking their own work may run the tree's own `flows/` the same way
(`MAIN=.`); that run's gate is the change's own copy, which the report names,
and it never counts for landing.

Run it from the repository root with CI's bun (1.4.2) and node (24,
`.github/workflows/ci.yml`) first on PATH and Docker running. It tests a
commit: a tree with tracked edits is refused, untracked files make the run
`needs_human`, and the report names the commit. It writes
`.relayflow/ci/report.md`, and empties that directory only when it made it.

1. **Classify.** `flows/lib/classify.mjs` decides how much the change needs,
   by path rules over the commit's changes since the merge base:

   | Verdict | When | Runs |
   | --- | --- | --- |
   | `none` | plain docs under `docs/` (Markdown and images) that no tracked code file names; never `docs/contracts/`, `docs/decisions/`, `docs/handoffs/` or `docs/packets/` | the guards |
   | `targeted` | root Markdown (AGENTS.md is policy), `agent/` (a deployed prompt), `runbooks/`, the decisions, a doc a test or script reads, or code in one contained area (`src/bob-costas`, `src/referee`, `src/alleyoop`, `scripts/` outside pg-proof and ci, ...) | the guards, typecheck, the unit shards and aux, and the e2e steps those paths reach (possibly none) |
   | `full` | the ingest and any ledger, HoopDb, `supabase/`, contracts, the consumer, the read API, erase, acquisition, migration (the erasure executor), pg-proof and the shard plan, shared and vendored code, the stacks, the services image, the e2e harness, dependencies, workflows, every file under `flows/`, or any path no rule knows | all of the above, pg-proof on PostgreSQL 15/16/17, and the whole pipeline e2e |

   `--input '{"jev":true}'` also asks Jev (TypeSafe's closed-choice
   classifier) with the changed paths and diff stat. Jev can only escalate,
   and only at confidence 0.8 or more; it never lowers the path rules'
   verdict. Its token comes from `JEV_TOKEN` in the environment; without it
   the step is skipped and the report says so. `--input '{"verdict":"full"}'`
   raises the verdict by hand (never lowers it).
2. **Lanes, side by side** (`flows/lib/lane.sh`, the same commands as
   `.github/workflows/ci.yml` and `pg-proof.yml`, each disposable container on
   a loopback port and removed afterwards), in five jobs:
   - `checks`: no Helm hooks, nothing secret-like tracked, no production
     identifiers, the lockfiles; then typecheck, the aux suite, and the flows'
     own check when `flows/` changed;
   - `shards 1+3`, `shards 2+4` (each shard with its own disposable ClickHouse);
   - `pg-proof`: pg-proof's real-PostgreSQL job on 15, 16 and 17 in turn;
   - `e2e`: the full-pipeline e2e, [docs/pipeline-e2e.md](pipeline-e2e.md), on
     a fresh compose project with its own image tag, or with
     `"e2eTarget":"sandbox"` in a throwaway Blaxel microVM (`BL_API_KEY` and
     `BL_WORKSPACE` from the environment).
3. **The GitHub gate** (`"github": true`, for every verdict): `flows/lib/remote-ci.mjs`
   snapshots the commit, refuses to publish (see Trust), pushes the snapshot to
   a throwaway `ci/<sha>` branch, dispatches `ci.yml` and `pg-proof.yml` there
   (`workflow_dispatch`), waits up to two hours (cancelling what is still
   running at the deadline), and deletes the branch. That is where the AWS
   hardening mutations, the AMP transport image, the session read Worker and
   pg-proof's other jobs run. The push triggers on `main` and
   `release/schema-*` are unchanged: the schema release anchors read those
   push runs. Until the `workflow_dispatch` triggers are on main, a
   `{"github":true}` run ends `needs_human` rather than claim the gate.

The report heads with the commit, the gate's origin, the verdict and its
reasons, then one PASS/FAIL line per lane, the e2e's own per-step report, and
the output tail of each red lane.
