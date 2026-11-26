# Session Intelligence overlap benchmark

A benchmark for **overlap and rework**: two lines of work, usually by different people's agents, converging on one change without knowing it. It measures, on real workspace data, how early and how precisely a detector could have warned. Its inputs are a cloud harvest ([session-intel-cloud-harvest.md](session-intel-cloud-harvest.md)) and forge outcomes ([session-intel-forge-outcomes.md](session-intel-forge-outcomes.md)). Its baseline is the shipped `outcome-facts` detector ([coordination-overlap-detection.md](coordination-overlap-detection.md)).

Code: `src/session-intel/overlap-bench/`. Tests: `src/session-intel/__tests__/overlap-bench.test.ts` and `overlap-eval.test.ts`.

All harvested data, labels, verdicts and bundles stay outside this repository. The code and these docs carry only aggregate numbers.

## Model

`model.ts` builds sessions from the harvest and groups them into **work roots**, following same-source parents, so a lead and its subagents are one line of work. Each session carries:

- time-ordered edits, each reduced to a repository and a repo-relative path;
- PR references, each a `create` or a `mention`;
- the branches and shas it named;
- the relay messages it received.

Events stamped before 2025 or with unparseable times are dropped.

**PR ownership.** A root owns a PR only when it created it or created or pushed its head branch, and only from that call's time (`prOwnership`). A `gh pr create` call usually has no number; the first full PR URL within 10 minutes after it claims the number. A root that only names a PR (a view, a review, a merge, a merge train listing it) does not own it. Otherwise one reviewer touching two PRs would make both look like one line of work. Detectors and as-of bundles ask who owned a PR as of the alert (`ownersAt`). Only scoring and hindsight adjudication use every owner a PR ever had.

**Observed paths.** `git status` and `--stat` output list what is dirty or changed, not what this session wrote. `ObservedPolicy` sets how they count:

- `all` counts them as edits (the default);
- `fresh` keeps only status paths the session's first status did not already list;
- `none` drops them.

On the dev split (provisional, see "Findings so far"), observed paths are 84% of recovered edit events. Dropping them does not change the ceiling: the combined ranker reaches 6 dev incidents with no budget under every policy. At the headline budget, `fresh` moves the ranker from 5 hits and 2 negatives to 4 hits and 0 negatives. With samples this small that is not a clear win, so the default stays `all`.

**Relay suppression.** A pair that communicated over Agent Relay before an alert is coordinated, not overlapping. `communicated` keys this by work root, so a lead messaging a worker covers both roots.

## Paths

`paths.ts` reduces every written path to `(repo, repo-relative path)` across checkouts, worktrees (`.claude/worktrees`, `.codex/worktrees`, `worktrees/` and others) and already-relative paths. It drops paths outside a repository, such as scratch directories, dotfiles and other repositories. `HOT_PATH` marks lockfiles, migration journals, snapshots and changelogs, which every line touches.

## Detectors

A **side** is a work root (`root:<session key>`) or a pull request (`pr:<owner/repo>#<n>`). Every detector is a pure function of the model, the forge state and its params. It never fires earlier than its evidence.

| Detector | Fires when |
|---|---|
| D1 `open-pr` | A root edits `minShared` files that another line's open PR had worked on by then |
| D2 `shared-trigger` | Two roots react to the same PR they do not own, opening a PR or editing within `windowMin` of naming it |
| D3 `concurrent` | Two roots edit the same uncommon paths in one repository within `windowH` hours. A path that more than `maxRootsPerPath` roots have edited is treated as hot. |
| `outcome-facts` | The shipped detector, run unchanged over the same evidence mapped onto its `LaneFacts` (`baseline.ts`) |

**Dated evidence only.** Every detector reads only what is dated before its alert. For D1 this means a PR's final file list is never used, because it is known only in hindsight. What a PR is working on at time t is the set of paths its owners (as of t) had edited before t, and `maxPrFiles` applies to that set. A PR with no owning root gives D1 no evidence.

D3's score sums each shared path's rarity, which is 1 over the number of roots that had edited the path by then. Lines meeting on files few others touch outrank lines meeting on a common file.

## Combined ranker

`ranker.ts` (D4) merges D1, D2 and D3 evidence per pair of lines and sums the weights in time order. A pair alerts when its running total first reaches `threshold`, with that total as its score, so only evidence seen by then counts and lead time stays honest. A D1 alert pairs a root with a PR; when the PR has an owning root, the alert is also counted against that root. The component detectors run loose and the ranker does the selecting. Its dev grid has 243 points.

## Scoring

Incidents are pairs of forge lines (a PR or a commit). A line links to its PR node and to every work root that created, named or pushed it. An alert **hits** an incident when its two sides link to the two lines and it fires before the harm. An alert on an adjudicated negative (a landing race, self-iteration, process) counts as a **negative hit**.

Alerts are first cut to a **weekly budget** of `perHundred` alerts per 100 coding sessions active that week, highest score first. A global budget would let one noisy week spend everyone else's alerts.

## Eval

`eval.ts` reports, for each detector point:

- **Recall curve.** Hits, negative hits, alerts and median lead time at budgets of 1, 5 (the headline budget), 20 and unlimited per 100 sessions.
- **Precision at the headline budget.** This comes from blind verdicts on the alerts themselves, with a 95% Wilson interval. The incident labels cover only overlaps the forge shows, so they cannot measure precision. It is conditional: the rate among the judged alerts, with unclear verdicts excluded. The sample is drawn from each detector's top-ranked alerts, so it estimates precision at the top of the ranking, not across every alert the budget admits. `unjudged` reports how many were not judged.
- **Ceiling.** Each incident is classed as `no_session` (no session evidence for either line), `one_side` or `both_sides`. A miss in the first two classes is the data's fault, not the detector's.

```
bun run src/session-intel/overlap-bench/cli.ts climb   --harvest <dir>[,<dir>] --forge <dir> --labels <labels.json> --out <dir>
bun run src/session-intel/overlap-bench/cli.ts eval    ... --frozen <frozen.json> [--verdicts <verdicts.json>]
bun run src/session-intel/overlap-bench/cli.ts sample  ... --frozen <frozen.json> [--verdicts <verdicts.json>] [--per-detector 15] [--seed v1] [--owner-labels <file>]
bun run src/session-intel/overlap-bench/cli.ts heldout ... --frozen <frozen.json>
```

## Precision

`sample` writes an adjudication bundle. It takes the top alerts of each frozen point at the headline budget, merges them by pair, skips pairs already judged, and shuffles them by a seeded hash. Detector names are withheld so a verdict cannot lean on them.

`bundle.ts` describes each pair from derived facts and forge metadata only: the PRs each side created, their titles, files and outcomes, branches, and edited files with times. Owners are shown as labels from a private file; anyone else is `member`. This hindsight form is for adjudicating what really happened.

With `asOf`, the bundle carries dated facts only, as a live system would have known them when the alert fired:

- the PRs that existed and that the side owned by then, with their author and creation time;
- comments made before the alert (each comment is dated);
- the side's own branches, edits and activity up to then.

A PR's title, description and file list are left out, and so are merges and closes. They are known only as fetched, so a later edit would leak hindsight. A judge that reads intent from as-of bundles therefore reads it from branches, edits and dated comments.

## Protocol

1. Climb on the dev split only. Labels are split by workstream, 44 dev incidents and 20 held-out.
2. Freeze the best point per detector (`overlap-bench-frozen/2`). The freeze records each point's hash (detector, params and sources) together with:
   - the sources hash;
   - the labels digest;
   - a digest of every input file (each harvest's directory and facts, and the forge PR files);
   - the alert budget;
   - the split scored (`heldout`);
   - the observed-path policy.
3. Score held-out once. `heldout` recomputes every term and every point hash and refuses any mismatch. It uses the frozen budget (a different `--per-hundred` is refused), and refuses if a ledger for these labels already exists (the ledger is written with `wx`).
4. Precision samples use fresh seeds, and a pair is never judged twice.

## Findings so far (dev split)

These come from a Jul 28 → Nov 5 harvest of one workspace: about 5.2M events, with no read errors.

**Status: provisional, pending a rerun.** The figures below were produced by revision 144b4ab2, before the review round that added the window, path and dated-evidence rules above. That revision had these known problems:
- D1 used final PR file lists;
- ownership was taken from full history;
- as-of bundles carried PR titles, descriptions and file lists as fetched;
- harvested events outside the window, and some non-path strings, were kept.

The D1, ranker and stage-2 numbers are withdrawn until they are rerun. The other figures are kept as orientation only. Use of the harvested outputs is suspended while their disposition is decided, so the rerun waits on that decision.

**Ceiling.** Of the 44 dev incidents, 19 have no session evidence on either side and 17 have it on one side only. Only 8 have both sides. Most misses are coverage, not detection.

**Recall (orientation only).** Hits out of 23 linkable incidents, with negative hits in brackets. D1 and the ranker are withdrawn, as above.

| Detector | per 100 = 1 | 5 | 20 | unlimited |
|---|---|---|---|---|
| `outcome-facts` (shipped) | 0 | 0 | 0 | 0 |
| D2 `shared-trigger` | 0 (1) | 3 (2) | 4 (6) | 4 (6) |
| D3 `concurrent` | 0 (1) | 1 (1) | 2 (1) | 5 (4) |

**Precision (orientation only).** In 73 blind-judged top alerts, judged in hindsight, 1 was an overlap, 3 were unclear and 69 were planned related work: stacked and follow-up PRs, handoffs, restarts, companion halves of one feature. Structure finds the right neighbourhood but cannot tell planned work from duplicated work.

**Naming and reference features do not separate them either.** PRs referencing each other, concurrency, a shared relay lead, and title and branch token similarity were each tried. None separated the two classes, and the experiment was not kept.

**An as-of intent judge (withdrawn).** A first blind test suggested that a judge reading each pair as of its alert separates duplicated from planned work, where structure cannot. Its bundles included PR titles and descriptions as fetched, which the strict as-of rules now exclude, so its result is withdrawn. It has to be rerun on strict as-of bundles, and on held-out, before it supports any claim.

## Stage 2 (proposed)

The provisional evidence points to two stages:

1. A cheap structural **recall** stage: the D4 ranker at a loose budget.
2. An **intent judge** on each candidate's `asOf` bundle.

Stage 2 should be the existing conflict model-judge seam ([session-intel-conflicts.md](session-intel-conflicts.md)), not a second judge. That seam provides an injected transport, verdicts that must cite their inputs, and no key read by the module.

Before it is wired anywhere, it needs:

- a versioned prompt and a structured verdict, frozen by hash with stage 1;
- one held-out score: the 20 held-out incidents plus a fresh negative sample.

Enabling it in production needs Khaliq's explicit OK. At the loose budget it would cost roughly 60 judge calls a day for this workspace.

## For production

These findings apply to the live overlap path, and each change goes through review separately:

- **Edit evidence.** Lane writes come only from `filesTouched` (`src/session-read-api/coordination-backend.ts`), which most harness sessions lack. Recovering writes from commands (`patch`, `add`, `shell`) would cover far more lines of work.
- **PR ownership.** Ownership should come from the create result, not from mentions.
- **Relay suppression.** Pairs that communicated over Agent Relay should not alert.
- **Budget.** The alert budget should be weekly.
