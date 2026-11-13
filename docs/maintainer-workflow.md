# Maintainer workflow

How changes to this repository are reviewed and landed by the maintainers and
the agents they assign. Adapted from relay-app's `docs/maintainer-workflow.md`;
the review flows, the flows library and the CI flow's structure come from
relay-app's `flows/` (provenance in [flows/README.md](../flows/README.md)).

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
2. **Commit and push** the change to `review/<your-name>-<slug>`. Never open a
   pull request: pull requests are switched off on this repository by the
   owner's decision and stay off.
3. **Ask `layered-sessions-pipeline` to review it**, and say what was asked for.
   - It runs one review round together with the CI flow, and sends back the
     findings.
   - Reviews run as Agent Relay Cloud flow runs (`flows/cloud-review.flow.ts`,
     with the lenses and briefs of `flows/adversarial-review.flow.ts` and
     `flows/context-review.flow.ts`), and the Cloud run ID and report are kept
     with the round. If Cloud refuses a run, that is reported as a failed
     round. It is never run locally as a fallback.
   - Fix all the findings together and push once, not once per finding. A fix
     push fixes the reported findings and nothing else; anything unrelated
     goes in its own branch with its own round.
   - A fix push gets the CI flow and the GitHub gate only, not another full
     review.
   - Only a blocker finding (something that must not ship) gets a re-review,
     and it covers just the fix.
4. **Landing.** `layered-sessions-pipeline` squashes the change onto `main`
   when all three hold:
   - the change's one review round has reported. A refused or unfinished
     round is not a report; a refused Cloud run is retried or escalated to
     the owner, never skipped;
   - no blocker is open, and the fix for any blocker has been re-reviewed;
   - the CI flow, with the GitHub gate, is green on the commit being landed.

   The author can say "I land this myself" or "hold this" when asking for the
   review or at any point before the landing. A request or quote relayed by
   another agent neither brings a landing forward nor holds one back.
   - Findings that are not blockers do not hold a landing. They are fixed
     forward in a follow-up branch on `main`, each with its own single round.
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

## The CI flow

```sh
npm --prefix flows ci                     # once per checkout
flows/node_modules/.bin/flows run --local-agent flows/ci.flow.ts --input '{}'                every round
flows/node_modules/.bin/flows run --local-agent flows/ci.flow.ts --input '{"github":true}'   the final gate
```

Run it from the repository root with CI's bun (1.4.2, `.github/workflows/ci.yml`)
first on PATH and Docker running. It writes `.relayflow/ci/report.md`.

1. **Classify.** `flows/lib/classify.mjs` decides how much the change needs,
   by path rules over everything changed since the merge base:

   | Verdict | When | Runs |
   | --- | --- | --- |
   | `none` | docs only (Markdown anywhere, images under `docs/`) | the guards |
   | `targeted` | code in one contained area (`src/bob-costas`, `src/referee`, `src/alleyoop`, `scripts/`, `flows/`, ...) | the guards, typecheck, the unit shards and aux, and the e2e steps those paths reach (possibly none) |
   | `full` | the ingest and any ledger, HoopDb, `supabase/`, the consumer, the read API, erase, shared and vendored code, the stacks, the services image, the e2e harness, dependencies, workflows, the CI gate itself, or any path no rule knows | all of the above, pg-proof on PostgreSQL 15/16/17, and the whole pipeline e2e |

   `--input '{"jev":true}'` also asks Jev (TypeSafe's closed-choice
   classifier) with the changed paths and diff stat. Jev can only escalate,
   and only at confidence 0.8 or more; it never lowers the path rules'
   verdict. Its token comes from `JEV_TOKEN` in the environment; without it
   the step is skipped and the report says so. `--input '{"verdict":"full"}'`
   raises the verdict by hand (never lowers it).
2. **Lanes, side by side** (`flows/lib/lane.sh`, the same commands as
   `.github/workflows/ci.yml` and `pg-proof.yml`, each disposable container on
   a loopback port and removed afterwards):
   - `guards`: no Helm hooks, nothing secret-like tracked, no production
     identifiers;
   - `typecheck`, `shard 1`..`shard 4` (each with its own disposable
     ClickHouse), `aux`;
   - `pg-proof 15`, `16`, `17`: pg-proof's real-PostgreSQL job;
   - `flows`: the flows' typecheck, `flows check` and helper tests, when
     `flows/` changed;
   - `e2e`: the full-pipeline e2e, [docs/pipeline-e2e.md](pipeline-e2e.md), on
     a fresh compose project here, or with `"e2eTarget":"sandbox"` in a
     throwaway Blaxel microVM (`BL_API_KEY` and `BL_WORKSPACE` from the
     environment).
3. **The GitHub gate** (`"github": true`): `flows/lib/remote-ci.mjs` snapshots
   the working tree, refuses to push machine-local state, secret-like files or
   production identifiers (the repository is public), pushes the snapshot to a
   throwaway `ci/<sha>` branch, dispatches `ci.yml` and `pg-proof.yml` there
   (`workflow_dispatch`), waits, and deletes the branch. That is where the AWS
   hardening mutations, the AMP transport image, the session read Worker and
   pg-proof's other jobs run. The push triggers on `main` and
   `release/schema-*` are unchanged: the schema release anchors read those
   push runs.

The report heads with the verdict and its reasons, then one PASS/FAIL line per
lane, the e2e's own per-step report, and the output tail of each red lane.
